﻿using Microsoft.SqlServer.Dts.Runtime;
using Microsoft.SqlServer.Dts.Runtime.Enumerators.ADO;
using Microsoft.SqlServer.SSIS.EzAPI;
using System.Collections.Generic;
using System.Composition;
using System.Linq;

namespace ConsoleApp1
{
    public interface ICreatePackage
    {
        Dictionary<int, EzOleDbConnectionManager> ezOleDbConnectionManagers { get; set; }
        EzPackage Create(Package pk);
    }


    [Export("Replenish", typeof(ICreatePackage))]
    public class CreateReplenishPackage : ICreatePackage
    {
        public Dictionary<int, EzOleDbConnectionManager> ezOleDbConnectionManagers { get; set; }
        public EzPackage Create(Package pk)
        {
            EzPackage ezPackage = new EzPackage
            {
                Name = pk.PackageName
            };
            ///添加变量
            ezPackage.Variables.Add("ExecuteId", false, "User", "0");
            ezPackage.Variables.Add("TimeList", false, "User", null);
            ezPackage.Variables.Add("StartTime", false, "User", "");
            ezPackage.Variables.Add("EndTime", false, "User", "");
            //变量集合
            Variables Variables = ezPackage.Variables;
            IList<Task> tasks;
            using (AutossisEntities db = new AutossisEntities())
            {
                tasks = db.Task.Where(p => p.PackageId == pk.PackageId).ToList();
            }
            #region 第0步:GetTimeList
            Task GetTimeListTask = tasks.Where(p => p.TaskName.StartsWith("GetTimeList")).First();
            EzExecSqlTask GetTimeList = createEzExecSqlTask(ezPackage, GetTimeListTask, null);
            GetTimeList.ResultSetType = Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask.ResultSetType.ResultSetType_Rowset;
            GetTimeList.ParameterBindings.Add(Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask.ParameterDirections.Output, "0", "User::StartTime", OleDBDataTypes.NVARCHAR);
            GetTimeList.ParameterBindings.Add(Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask.ParameterDirections.Output, "1", "User::EndTime", OleDBDataTypes.NVARCHAR);
            GetTimeList.ResultBindings.Add("0", "User::TimeList");
            #endregion

            #region 第1步:Log_PackageStart
            Task Log_PackageStartTask = tasks.Where(p => p.TaskName.StartsWith("Log_PackageStart")).First();
            EzExecSqlTask Log_PackageStart = createEzExecSqlTask(ezPackage, Log_PackageStartTask, GetTimeList);
            Log_PackageStart.ResultSetType = Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask.ResultSetType.ResultSetType_SingleRow;
            Log_PackageStart.ResultBindings.Add("0", "User::ExecuteId");
            #endregion

            #region 第2步:truncate StagTable
            Task truncateTask = tasks.Where(p => p.TaskName.StartsWith("truncate")).First();
            EzExecSqlTask SQLTaskTruncate = createEzExecSqlTask(ezPackage, truncateTask, Log_PackageStart);
            #endregion

            #region  第3步 创建foreach容器
            EzForEachLoop ForeachContainer = CreateEzForEachLoop(ezPackage, SQLTaskTruncate);
            #endregion

            #region 第3.1步:Log_LoadDataToStag
            Task Log_LoadDataToStagTask = tasks.Where(p => p.TaskName.StartsWith("Log_LoadDataToStag")).First();
            EzExecSqlTask Log_LoadDataToStag = createEzExecSqlTask(ForeachContainer, Log_LoadDataToStagTask, null);
            Log_LoadDataToStag.ParameterBindings.Add("0", "User::ExecuteId", OleDBDataTypes.NVARCHAR);
            Log_LoadDataToStag.ParameterBindings.Add("1", "User::StartTime", OleDBDataTypes.NVARCHAR);
            #endregion

            #region 第3.2步 创建序列容器
            EzSequence ezContainer = CreateEzSequence(ForeachContainer, Log_LoadDataToStag);
            #endregion

            #region 第3.2.1步：load data
            List<Task> dataflowTasks = tasks.Where(p => p.TaskName.StartsWith("load data") && p.SourceTableName.TrimEnd() != "").ToList();
            foreach (Task dft in dataflowTasks)
            {
                createEzDataFlow(ezContainer, dft, null, Variables);
            }
            #endregion

            #region 第3.3步:Log_MergeData
            Task Log_MergeDataTask = tasks.Where(p => p.TaskName.StartsWith("Log_MergeData")).First();
            EzExecSqlTask Log_MergeData = createEzExecSqlTask(ForeachContainer, Log_MergeDataTask, ezContainer);
            Log_MergeData.ParameterBindings.Add("0", "User::ExecuteId", OleDBDataTypes.NVARCHAR);
            Log_MergeData.ParameterBindings.Add("1", "User::StartTime", OleDBDataTypes.NVARCHAR);
            #endregion

            #region 第3.4步：merge data
            Task mergeTask = tasks.Where(p => p.TaskName.StartsWith("merge")).First();
            EzExecSqlTask SQLTaskMerge = createEzExecSqlTask(ForeachContainer, mergeTask, Log_MergeData);
            #endregion

            #region 第4步:Log_PackageEnd
            Task Log_PackageEndTask = tasks.Where(p => p.TaskName.StartsWith("Log_PackageEnd")).First();
            EzExecSqlTask Log_PackageEnd = createEzExecSqlTask(ezPackage, Log_PackageEndTask, ForeachContainer);
            Log_PackageEnd.ParameterBindings.Add("0", "User::ExecuteId", OleDBDataTypes.NVARCHAR);
            #endregion

            return ezPackage;
        }

        /// <summary>
        /// 创建执行SQL任务
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="task"></param>
        /// <param name="PreviousComponent"></param>
        /// <returns></returns>
        private EzExecSqlTask createEzExecSqlTask(EzContainer parent, Task task, EzExecutable PreviousComponent)
        {
            EzExecSqlTask ezExecSqlTask = new EzExecSqlTask(parent);
            if (PreviousComponent != null)
            {
                ezExecSqlTask.AttachTo(PreviousComponent);
            }
            ezExecSqlTask.Name = task.TaskName;
            ezExecSqlTask.Connection = ezOleDbConnectionManagers[task.SourceConmgrId]; //EzOleDbConMgrSource;
            ezExecSqlTask.SqlStatementSourceType = Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask.SqlStatementSourceType.DirectInput;
            ezExecSqlTask.SqlStatementSource = task.SqlCommand;
            return ezExecSqlTask;
        }

        /// <summary>
        /// 创建 数据流任务
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="task"></param>
        /// <param name="PreviousComponent"></param>
        /// <returns></returns>
        private EzDataFlow createEzDataFlow(EzContainer parent, Task task, EzExecutable PreviousComponent, Variables Variables)
        {
            //Adding a data flow task
            EzDataFlow dataflow = new EzDataFlow(parent);
            if (PreviousComponent != null)
            {
                dataflow.AttachTo(PreviousComponent);
            }
            dataflow.Name = task.TaskName + "_" + task.TaskId;
            EzOleDbSource source = new EzOleDbSource(dataflow)
            {
                Name = task.TaskName,
                //source.SqlCommand =string.Format("exec [dbo].[{0}] ",task.SourceTableName);
                SqlCommand = task.SourceTableName,


                Connection = ezOleDbConnectionManagers[task.SourceConmgrId],
                //source.Table = task.SourceTableName;
                AccessMode = AccessMode.AM_SQLCOMMAND
            };

            string start_guid = "";
            string end_guid = "";

            foreach (Variable x in Variables)
            {

                if (x.Namespace == "User")
                {
                    if (x.Name == "StartTime" || x.Name == "EndTime")
                    {
                        if (x.Name == "StartTime")
                        {
                            start_guid = x.ID;
                            if (end_guid != "")
                            {
                                break;
                            }
                        }
                        else
                        {
                            end_guid = x.ID;
                            if (start_guid != "")
                            {
                                break;
                            }
                        }
                    }
                }
            }
            source.SetComponentProperty("ParameterMapping", "\"@StartTime:Input\"," + start_guid + ";\"@EndTime:Input\"," + end_guid + ";");
            //Adding an OLE DB Destination
            EzOleDbDestination destination = new EzOleDbDestination(dataflow)
            {
                Name = task.TargetTableName,
                Connection = ezOleDbConnectionManagers[task.TargetConmgrId],
                AccessMode = AccessMode.AM_OPENROWSET_FASTLOAD,
                Table = task.TargetTableName
            };

            //Linking source and destination
            destination.AttachTo(source);
            destination.LinkAllInputsToOutputs();
            return dataflow;
        }

        /// <summary>
        /// 创建 序列容器
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="PreviousComponent"></param>
        /// <returns></returns>
        private EzSequence CreateEzSequence(EzContainer parent, EzExecutable PreviousComponent)
        {
            EzSequence ezContainer = new EzSequence(parent);
            if (PreviousComponent != null)
            {
                ezContainer.AttachTo(PreviousComponent);
            }
            ezContainer.Name = "Data Flows";
            return ezContainer;
        }

        /// <summary>
        /// 创建 foreach容器
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="PreviousComponent"></param>
        /// <returns></returns>
        private EzForEachLoop CreateEzForEachLoop(EzContainer parent, EzExecutable PreviousComponent)
        {
            EzForEachLoop ForEachContainer = new EzForEachLoop(parent)
            {
                Name = "Replenish data by month"
            };
            if (PreviousComponent != null)
            {
                ForEachContainer.AttachTo(PreviousComponent);
            }
            ForEachContainer.Initialize(ForEachEnumeratorType.ForEachADOEnumerator);
            //ForEachContainer.ForEachEnumerator = ForEachEnumeratorType.ForEachADOEnumerator;

            ForEachADOEnumerator ado_enum = (ForEachADOEnumerator)ForEachContainer.ForEachEnumerator.InnerObject;
            ado_enum.Type = ADOEnumerationType.EnumerateRowsInFirstTable;
            ado_enum.DataObjectVariable = "User::TimeList";

            ForEachContainer.VariableMappings.Add();
            ForEachContainer.VariableMappings[0].VariableName = "User::StartTime";
            ForEachContainer.VariableMappings[0].ValueIndex = "0";

            ForEachContainer.VariableMappings.Add();
            ForEachContainer.VariableMappings[1].VariableName = "User::EndTime";
            ForEachContainer.VariableMappings[1].ValueIndex = "1";


            //ezContainer.VariableMappings

            //EzForEachLoop ezf = new EzForEachLoop(this);
            //ForEachContainer.Initialize(ForEachEnumeratorType.ForEachADOEnumerator);
            return ForEachContainer;
        }

    }


    [Export("Full", typeof(ICreatePackage))]
    public class CreateFullPackage : ICreatePackage
    {
        public Dictionary<int, EzOleDbConnectionManager> ezOleDbConnectionManagers { get; set; }
        public EzPackage Create(Package pk)
        {
            return null;
        }

    }

    [Export("Inc", typeof(ICreatePackage))]
    public class CreateIncPackage : ICreatePackage
    {
        public Dictionary<int, EzOleDbConnectionManager> ezOleDbConnectionManagers { get; set; }
        public EzPackage Create(Package pk)
        {
            return null;
        }

    }
}