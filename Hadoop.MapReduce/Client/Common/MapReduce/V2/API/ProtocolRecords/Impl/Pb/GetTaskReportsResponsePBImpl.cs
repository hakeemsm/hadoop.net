using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Mapreduce.V2.Proto;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords.Impl.PB
{
	public class GetTaskReportsResponsePBImpl : ProtoBase<MRServiceProtos.GetTaskReportsResponseProto
		>, GetTaskReportsResponse
	{
		internal MRServiceProtos.GetTaskReportsResponseProto proto = MRServiceProtos.GetTaskReportsResponseProto
			.GetDefaultInstance();

		internal MRServiceProtos.GetTaskReportsResponseProto.Builder builder = null;

		internal bool viaProto = false;

		private IList<TaskReport> taskReports = null;

		public GetTaskReportsResponsePBImpl()
		{
			builder = MRServiceProtos.GetTaskReportsResponseProto.NewBuilder();
		}

		public GetTaskReportsResponsePBImpl(MRServiceProtos.GetTaskReportsResponseProto proto
			)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRServiceProtos.GetTaskReportsResponseProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRServiceProtos.GetTaskReportsResponseProto)builder.
				Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.taskReports != null)
			{
				AddTaskReportsToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((MRServiceProtos.GetTaskReportsResponseProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRServiceProtos.GetTaskReportsResponseProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public virtual IList<TaskReport> GetTaskReportList()
		{
			InitTaskReports();
			return this.taskReports;
		}

		public virtual TaskReport GetTaskReport(int index)
		{
			InitTaskReports();
			return this.taskReports[index];
		}

		public virtual int GetTaskReportCount()
		{
			InitTaskReports();
			return this.taskReports.Count;
		}

		private void InitTaskReports()
		{
			if (this.taskReports != null)
			{
				return;
			}
			MRServiceProtos.GetTaskReportsResponseProtoOrBuilder p = viaProto ? proto : builder;
			IList<MRProtos.TaskReportProto> list = p.GetTaskReportsList();
			this.taskReports = new AList<TaskReport>();
			foreach (MRProtos.TaskReportProto c in list)
			{
				this.taskReports.AddItem(ConvertFromProtoFormat(c));
			}
		}

		public virtual void AddAllTaskReports(IList<TaskReport> taskReports)
		{
			if (taskReports == null)
			{
				return;
			}
			InitTaskReports();
			Sharpen.Collections.AddAll(this.taskReports, taskReports);
		}

		private void AddTaskReportsToProto()
		{
			MaybeInitBuilder();
			builder.ClearTaskReports();
			if (taskReports == null)
			{
				return;
			}
			IEnumerable<MRProtos.TaskReportProto> iterable = new _IEnumerable_124(this);
			builder.AddAllTaskReports(iterable);
		}

		private sealed class _IEnumerable_124 : IEnumerable<MRProtos.TaskReportProto>
		{
			public _IEnumerable_124(GetTaskReportsResponsePBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<MRProtos.TaskReportProto> GetEnumerator()
			{
				return new _IEnumerator_127(this);
			}

			private sealed class _IEnumerator_127 : IEnumerator<MRProtos.TaskReportProto>
			{
				public _IEnumerator_127(_IEnumerable_124 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.taskReports.GetEnumerator();
				}

				internal IEnumerator<TaskReport> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override MRProtos.TaskReportProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_124 _enclosing;
			}

			private readonly GetTaskReportsResponsePBImpl _enclosing;
		}

		public virtual void AddTaskReport(TaskReport taskReports)
		{
			InitTaskReports();
			this.taskReports.AddItem(taskReports);
		}

		public virtual void RemoveTaskReport(int index)
		{
			InitTaskReports();
			this.taskReports.Remove(index);
		}

		public virtual void ClearTaskReports()
		{
			InitTaskReports();
			this.taskReports.Clear();
		}

		private TaskReportPBImpl ConvertFromProtoFormat(MRProtos.TaskReportProto p)
		{
			return new TaskReportPBImpl(p);
		}

		private MRProtos.TaskReportProto ConvertToProtoFormat(TaskReport t)
		{
			return ((TaskReportPBImpl)t).GetProto();
		}
	}
}
