using Microsoft.SqlServer.SSIS.EzAPI;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;

namespace ConsoleApp1
{
    class Program
    {
        static string projectName = ConfigurationManager.AppSettings.Get("ProjectName");
        static EzProject ez = new EzProject { Name = projectName };
        static AutossisEntities db = new AutossisEntities();
        static Project pj = db.Project.Where(p => p.ProjectName == projectName).First();
        static List<Conmgr> Conmgrs = db.Conmgr.Where(p => p.ProjectId == pj.ProjectId).ToList();
        static Dictionary<int, EzOleDbConnectionManager> ezOleDbConnectionManagers = new Dictionary<int, EzOleDbConnectionManager>();

        static void Main(string[] args)
        {
            Console.WriteLine("开始......");
            String ProjectPath = ConfigurationManager.AppSettings.Get("ProjectPath");
            foreach (var Conmgr in Conmgrs)
            {
                #region 创建项目连接 当前目录下必须存在同名的CgsOLEDBConnection.conmgr
                EzOleDbConnectionManager EzOleDbConMgrSource = new EzOleDbConnectionManager(ez, Conmgr.ConmgrName);
                EzOleDbConMgrSource.Name = Conmgr.ConmgrName;
                EzOleDbConMgrSource.ConnectionString = Conmgr.ConnectionString;
                EzOleDbConMgrSource.DelayValidation = true;
                ezOleDbConnectionManagers.Add(Conmgr.ConmgrId, EzOleDbConMgrSource);
                #endregion
            }
            var Packages = db.Package.Where(p => p.ProjectId ==pj.ProjectId);
            ICreatePackage icp = new CreatePackageFactory(db, ezOleDbConnectionManagers).getICreatePackage(ConfigurationManager.AppSettings.Get("ProjectType"));
            PackageFactory pf = new PackageFactory(icp);
            foreach (var package in Packages)
            {
                ez.AddPackage(pf.Create(package));
                Console.WriteLine(package.PackageName+" 生成成功。");
            }
            JobControl(null);
            ez.SaveTo(ProjectPath + pj.ProjectName + ".ispac");
            Console.WriteLine("全部成功！");
            Console.ReadKey();
        }
        
        /// <summary>
        /// 生成main包
        /// </summary>
        /// <param name="ParentInfo"></param>
        static void JobControl( PackageControl ParentInfo)
        {
            if (ParentInfo == null) {
                ParentInfo = db.PackageControl.Where(p => p.ProjectId == pj.ProjectId && p.ParentId == null).First();
            }

            var childs = db.PackageControl.Where(p => p.ParentId == ParentInfo.id);

            if (childs.Count() > 0)
            {
                var Package = new EzPackage { Name = ParentInfo.name };
                ez.AddPackage(Package);
                foreach (PackageControl child in childs)
                {
                    EzExecPackage exPack = new EzExecPackage(Package);
                    exPack.PackageName = child.name + ".dtsx";
                    exPack.UseProjectReference = true;
                    exPack.Name = child.name;
                    JobControl(child);
                }
            }
        }

    }
}
