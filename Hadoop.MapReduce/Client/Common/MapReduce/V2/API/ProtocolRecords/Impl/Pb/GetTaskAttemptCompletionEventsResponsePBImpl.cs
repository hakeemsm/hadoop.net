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
	public class GetTaskAttemptCompletionEventsResponsePBImpl : ProtoBase<MRServiceProtos.GetTaskAttemptCompletionEventsResponseProto
		>, GetTaskAttemptCompletionEventsResponse
	{
		internal MRServiceProtos.GetTaskAttemptCompletionEventsResponseProto proto = MRServiceProtos.GetTaskAttemptCompletionEventsResponseProto
			.GetDefaultInstance();

		internal MRServiceProtos.GetTaskAttemptCompletionEventsResponseProto.Builder builder
			 = null;

		internal bool viaProto = false;

		private IList<TaskAttemptCompletionEvent> completionEvents = null;

		public GetTaskAttemptCompletionEventsResponsePBImpl()
		{
			builder = MRServiceProtos.GetTaskAttemptCompletionEventsResponseProto.NewBuilder(
				);
		}

		public GetTaskAttemptCompletionEventsResponsePBImpl(MRServiceProtos.GetTaskAttemptCompletionEventsResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public override MRServiceProtos.GetTaskAttemptCompletionEventsResponseProto GetProto
			()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((MRServiceProtos.GetTaskAttemptCompletionEventsResponseProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		private void MergeLocalToBuilder()
		{
			if (this.completionEvents != null)
			{
				AddCompletionEventsToProto();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((MRServiceProtos.GetTaskAttemptCompletionEventsResponseProto)builder.Build
				());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = MRServiceProtos.GetTaskAttemptCompletionEventsResponseProto.NewBuilder(
					proto);
			}
			viaProto = false;
		}

		public virtual IList<TaskAttemptCompletionEvent> GetCompletionEventList()
		{
			InitCompletionEvents();
			return this.completionEvents;
		}

		public virtual TaskAttemptCompletionEvent GetCompletionEvent(int index)
		{
			InitCompletionEvents();
			return this.completionEvents[index];
		}

		public virtual int GetCompletionEventCount()
		{
			InitCompletionEvents();
			return this.completionEvents.Count;
		}

		private void InitCompletionEvents()
		{
			if (this.completionEvents != null)
			{
				return;
			}
			MRServiceProtos.GetTaskAttemptCompletionEventsResponseProtoOrBuilder p = viaProto
				 ? proto : builder;
			IList<MRProtos.TaskAttemptCompletionEventProto> list = p.GetCompletionEventsList(
				);
			this.completionEvents = new AList<TaskAttemptCompletionEvent>();
			foreach (MRProtos.TaskAttemptCompletionEventProto c in list)
			{
				this.completionEvents.AddItem(ConvertFromProtoFormat(c));
			}
		}

		public virtual void AddAllCompletionEvents(IList<TaskAttemptCompletionEvent> completionEvents
			)
		{
			if (completionEvents == null)
			{
				return;
			}
			InitCompletionEvents();
			Sharpen.Collections.AddAll(this.completionEvents, completionEvents);
		}

		private void AddCompletionEventsToProto()
		{
			MaybeInitBuilder();
			builder.ClearCompletionEvents();
			if (completionEvents == null)
			{
				return;
			}
			IEnumerable<MRProtos.TaskAttemptCompletionEventProto> iterable = new _IEnumerable_124
				(this);
			builder.AddAllCompletionEvents(iterable);
		}

		private sealed class _IEnumerable_124 : IEnumerable<MRProtos.TaskAttemptCompletionEventProto
			>
		{
			public _IEnumerable_124(GetTaskAttemptCompletionEventsResponsePBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<MRProtos.TaskAttemptCompletionEventProto> GetEnumerator
				()
			{
				return new _IEnumerator_127(this);
			}

			private sealed class _IEnumerator_127 : IEnumerator<MRProtos.TaskAttemptCompletionEventProto
				>
			{
				public _IEnumerator_127(_IEnumerable_124 _enclosing)
				{
					this._enclosing = _enclosing;
					this.iter = this._enclosing._enclosing.completionEvents.GetEnumerator();
				}

				internal IEnumerator<TaskAttemptCompletionEvent> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override MRProtos.TaskAttemptCompletionEventProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_124 _enclosing;
			}

			private readonly GetTaskAttemptCompletionEventsResponsePBImpl _enclosing;
		}

		public virtual void AddCompletionEvent(TaskAttemptCompletionEvent completionEvents
			)
		{
			InitCompletionEvents();
			this.completionEvents.AddItem(completionEvents);
		}

		public virtual void RemoveCompletionEvent(int index)
		{
			InitCompletionEvents();
			this.completionEvents.Remove(index);
		}

		public virtual void ClearCompletionEvents()
		{
			InitCompletionEvents();
			this.completionEvents.Clear();
		}

		private TaskAttemptCompletionEventPBImpl ConvertFromProtoFormat(MRProtos.TaskAttemptCompletionEventProto
			 p)
		{
			return new TaskAttemptCompletionEventPBImpl(p);
		}

		private MRProtos.TaskAttemptCompletionEventProto ConvertToProtoFormat(TaskAttemptCompletionEvent
			 t)
		{
			return ((TaskAttemptCompletionEventPBImpl)t).GetProto();
		}
	}
}
