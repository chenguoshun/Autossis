using Microsoft.SqlServer.SSIS.EzAPI;
using System;
using System.Collections.Generic;
using System.Composition.Convention;
using System.Composition.Hosting;
using System.Configuration;
using System.Linq;
using System.Reflection;

namespace ConsoleApp1
{
    internal class Program
    {
        private static string projectName = ConfigurationManager.AppSettings.Get("ProjectName");
        private static EzProject ez = new EzProject { Name = projectName };

        private static void Main(string[] args)
        {
            Console.WriteLine("开始......");
            string ProjectPath = ConfigurationManager.AppSettings.Get("ProjectPath");
            Project Pj;
            List<Conmgr> Conmgrs;
            List<Package> Packages;
            Dictionary<int, EzOleDbConnectionManager> CMS;
            using (AutossisEntities db = new AutossisEntities())
            {
                Pj = db.Project.Where(p => p.ProjectName == projectName).First();
                Conmgrs = db.Conmgr.Where(p => p.ProjectId == Pj.ProjectId).ToList();
                Packages = db.Package.Where(p => p.ProjectId == Pj.ProjectId).ToList();
                CMS = getOledbConnectionManagers(Conmgrs);
            }

            ConventionBuilder conventions = new ConventionBuilder();
            conventions
                .ForTypesDerivedFrom<ICreatePackage>()
                .Export<ICreatePackage>()
                .Shared();


            Assembly[] assemblies = new[] { typeof(Program).GetTypeInfo().Assembly };

            ContainerConfiguration configuration = new ContainerConfiguration()
                .WithAssemblies(assemblies, conventions);

            ICreatePackage Cp;
            using (CompositionHost container = configuration.CreateContainer())
            {
                Cp = container.GetExport<ICreatePackage>(ConfigurationManager.AppSettings.Get("ProjectType"));
                Cp.ezOleDbConnectionManagers = CMS;
            }
            foreach (Package package in Packages)
            {
                ez.AddPackage(Cp.Create(package));
                Console.WriteLine(package.PackageName + " 生成成功。");
            }
            JobControl(Pj, null);
            ez.SaveTo(ProjectPath + Pj.ProjectName + ".ispac");
            Console.WriteLine("全部成功！");
            Console.ReadKey();
        }

        /// <summary>
        /// 生成main包
        /// </summary>
        /// <param name="ParentInfo"></param>
        private static void JobControl(Project Pj, PackageControl ParentInfo)
        {
            IQueryable<PackageControl> childs;
            using (AutossisEntities db = new AutossisEntities())
            {
                if (ParentInfo == null)
                {

                    ParentInfo = db.PackageControl.Where(p => p.ProjectId == Pj.ProjectId && p.ParentId == null).First();
                }
                childs = db.PackageControl.Where(p => p.ParentId == ParentInfo.id);

            }

            if (childs.Count() > 0)
            {
                EzPackage Package = new EzPackage { Name = ParentInfo.name };
                ez.AddPackage(Package);
                foreach (PackageControl child in childs)
                {
                    EzExecPackage exPack = new EzExecPackage(Package)
                    {
                        PackageName = child.name + ".dtsx",
                        UseProjectReference = true,
                        Name = child.name
                    };
                    JobControl(Pj, child);
                }
            }
        }

        private static Dictionary<int, EzOleDbConnectionManager> getOledbConnectionManagers(List<Conmgr> Conmgrs)
        {
            Dictionary<int, EzOleDbConnectionManager> cms = new Dictionary<int, EzOleDbConnectionManager>();
            foreach (Conmgr Conmgr in Conmgrs)
            {
                #region 创建项目连接 当前目录下必须存在同名的CgsOLEDBConnection.conmgr
                EzOleDbConnectionManager EzOleDbConMgrSource = new EzOleDbConnectionManager(ez, Conmgr.ConmgrName)
                {
                    Name = Conmgr.ConmgrName,
                    ConnectionString = Conmgr.ConnectionString,
                    DelayValidation = true
                };
                cms.Add(Conmgr.ConmgrId, EzOleDbConMgrSource);
                #endregion
            }
            return cms;
        }


    }


}
