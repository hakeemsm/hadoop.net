using System;
using System.Collections.Generic;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB
{
	public class RegisterApplicationMasterResponsePBImpl : RegisterApplicationMasterResponse
	{
		internal YarnServiceProtos.RegisterApplicationMasterResponseProto proto = YarnServiceProtos.RegisterApplicationMasterResponseProto
			.GetDefaultInstance();

		internal YarnServiceProtos.RegisterApplicationMasterResponseProto.Builder builder
			 = null;

		internal bool viaProto = false;

		private Resource maximumResourceCapability;

		private IDictionary<ApplicationAccessType, string> applicationACLS = null;

		private IList<Container> containersFromPreviousAttempts = null;

		private IList<NMToken> nmTokens = null;

		private EnumSet<YarnServiceProtos.SchedulerResourceTypes> schedulerResourceTypes = 
			null;

		public RegisterApplicationMasterResponsePBImpl()
		{
			builder = YarnServiceProtos.RegisterApplicationMasterResponseProto.NewBuilder();
		}

		public RegisterApplicationMasterResponsePBImpl(YarnServiceProtos.RegisterApplicationMasterResponseProto
			 proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnServiceProtos.RegisterApplicationMasterResponseProto GetProto(
			)
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnServiceProtos.RegisterApplicationMasterResponseProto
				)builder.Build());
			viaProto = true;
			return proto;
		}

		public override int GetHashCode()
		{
			return GetProto().GetHashCode();
		}

		public override bool Equals(object other)
		{
			if (other == null)
			{
				return false;
			}
			if (other.GetType().IsAssignableFrom(this.GetType()))
			{
				return this.GetProto().Equals(this.GetType().Cast(other).GetProto());
			}
			return false;
		}

		public override string ToString()
		{
			return TextFormat.ShortDebugString(GetProto());
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnServiceProtos.RegisterApplicationMasterResponseProto)builder.Build(
				));
			viaProto = true;
		}

		private void MergeLocalToBuilder()
		{
			if (this.maximumResourceCapability != null)
			{
				builder.SetMaximumCapability(ConvertToProtoFormat(this.maximumResourceCapability)
					);
			}
			if (this.applicationACLS != null)
			{
				AddApplicationACLs();
			}
			if (this.containersFromPreviousAttempts != null)
			{
				AddContainersFromPreviousAttemptToProto();
			}
			if (nmTokens != null)
			{
				builder.ClearNmTokensFromPreviousAttempts();
				IEnumerable<YarnServiceProtos.NMTokenProto> iterable = GetTokenProtoIterable(nmTokens
					);
				builder.AddAllNmTokensFromPreviousAttempts(iterable);
			}
			if (schedulerResourceTypes != null)
			{
				AddSchedulerResourceTypes();
			}
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnServiceProtos.RegisterApplicationMasterResponseProto.NewBuilder(proto
					);
			}
			viaProto = false;
		}

		public override Resource GetMaximumResourceCapability()
		{
			if (this.maximumResourceCapability != null)
			{
				return this.maximumResourceCapability;
			}
			YarnServiceProtos.RegisterApplicationMasterResponseProtoOrBuilder p = viaProto ? 
				proto : builder;
			if (!p.HasMaximumCapability())
			{
				return null;
			}
			this.maximumResourceCapability = ConvertFromProtoFormat(p.GetMaximumCapability());
			return this.maximumResourceCapability;
		}

		public override void SetMaximumResourceCapability(Resource capability)
		{
			MaybeInitBuilder();
			if (maximumResourceCapability == null)
			{
				builder.ClearMaximumCapability();
			}
			this.maximumResourceCapability = capability;
		}

		public override IDictionary<ApplicationAccessType, string> GetApplicationACLs()
		{
			InitApplicationACLs();
			return this.applicationACLS;
		}

		private void InitApplicationACLs()
		{
			if (this.applicationACLS != null)
			{
				return;
			}
			YarnServiceProtos.RegisterApplicationMasterResponseProtoOrBuilder p = viaProto ? 
				proto : builder;
			IList<YarnProtos.ApplicationACLMapProto> list = p.GetApplicationACLsList();
			this.applicationACLS = new Dictionary<ApplicationAccessType, string>(list.Count);
			foreach (YarnProtos.ApplicationACLMapProto aclProto in list)
			{
				this.applicationACLS[ProtoUtils.ConvertFromProtoFormat(aclProto.GetAccessType())]
					 = aclProto.GetAcl();
			}
		}

		private void AddApplicationACLs()
		{
			MaybeInitBuilder();
			builder.ClearApplicationACLs();
			if (applicationACLS == null)
			{
				return;
			}
			IEnumerable<YarnProtos.ApplicationACLMapProto> values = new _IEnumerable_189(this
				);
			this.builder.AddAllApplicationACLs(values);
		}

		private sealed class _IEnumerable_189 : IEnumerable<YarnProtos.ApplicationACLMapProto
			>
		{
			public _IEnumerable_189(RegisterApplicationMasterResponsePBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ApplicationACLMapProto> GetEnumerator()
			{
				return new _IEnumerator_193(this);
			}

			private sealed class _IEnumerator_193 : IEnumerator<YarnProtos.ApplicationACLMapProto
				>
			{
				public _IEnumerator_193(_IEnumerable_189 _enclosing)
				{
					this._enclosing = _enclosing;
					this.aclsIterator = this._enclosing._enclosing.applicationACLS.Keys.GetEnumerator
						();
				}

				internal IEnumerator<ApplicationAccessType> aclsIterator;

				public override bool HasNext()
				{
					return this.aclsIterator.HasNext();
				}

				public override YarnProtos.ApplicationACLMapProto Next()
				{
					ApplicationAccessType key = this.aclsIterator.Next();
					return ((YarnProtos.ApplicationACLMapProto)YarnProtos.ApplicationACLMapProto.NewBuilder
						().SetAcl(this._enclosing._enclosing.applicationACLS[key]).SetAccessType(ProtoUtils
						.ConvertToProtoFormat(key)).Build());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_189 _enclosing;
			}

			private readonly RegisterApplicationMasterResponsePBImpl _enclosing;
		}

		public override void SetApplicationACLs(IDictionary<ApplicationAccessType, string
			> appACLs)
		{
			if (appACLs == null)
			{
				return;
			}
			InitApplicationACLs();
			this.applicationACLS.Clear();
			this.applicationACLS.PutAll(appACLs);
		}

		public override void SetClientToAMTokenMasterKey(ByteBuffer key)
		{
			MaybeInitBuilder();
			if (key == null)
			{
				builder.ClearClientToAmTokenMasterKey();
				return;
			}
			builder.SetClientToAmTokenMasterKey(ByteString.CopyFrom(key));
		}

		public override ByteBuffer GetClientToAMTokenMasterKey()
		{
			MaybeInitBuilder();
			ByteBuffer key = ByteBuffer.Wrap(builder.GetClientToAmTokenMasterKey().ToByteArray
				());
			return key;
		}

		public override IList<Container> GetContainersFromPreviousAttempts()
		{
			if (this.containersFromPreviousAttempts != null)
			{
				return this.containersFromPreviousAttempts;
			}
			InitContainersPreviousAttemptList();
			return this.containersFromPreviousAttempts;
		}

		public override void SetContainersFromPreviousAttempts(IList<Container> containers
			)
		{
			if (containers == null)
			{
				return;
			}
			this.containersFromPreviousAttempts = new AList<Container>();
			Sharpen.Collections.AddAll(this.containersFromPreviousAttempts, containers);
		}

		public override string GetQueue()
		{
			YarnServiceProtos.RegisterApplicationMasterResponseProtoOrBuilder p = viaProto ? 
				proto : builder;
			if (!p.HasQueue())
			{
				return null;
			}
			return p.GetQueue();
		}

		public override void SetQueue(string queue)
		{
			MaybeInitBuilder();
			if (queue == null)
			{
				builder.ClearQueue();
			}
			else
			{
				builder.SetQueue(queue);
			}
		}

		private void InitContainersPreviousAttemptList()
		{
			YarnServiceProtos.RegisterApplicationMasterResponseProtoOrBuilder p = viaProto ? 
				proto : builder;
			IList<YarnProtos.ContainerProto> list = p.GetContainersFromPreviousAttemptsList();
			containersFromPreviousAttempts = new AList<Container>();
			foreach (YarnProtos.ContainerProto c in list)
			{
				containersFromPreviousAttempts.AddItem(ConvertFromProtoFormat(c));
			}
		}

		private void AddContainersFromPreviousAttemptToProto()
		{
			MaybeInitBuilder();
			builder.ClearContainersFromPreviousAttempts();
			IList<YarnProtos.ContainerProto> list = new AList<YarnProtos.ContainerProto>();
			foreach (Container c in containersFromPreviousAttempts)
			{
				list.AddItem(ConvertToProtoFormat(c));
			}
			builder.AddAllContainersFromPreviousAttempts(list);
		}

		public override IList<NMToken> GetNMTokensFromPreviousAttempts()
		{
			if (nmTokens != null)
			{
				return nmTokens;
			}
			InitLocalNewNMTokenList();
			return nmTokens;
		}

		public override void SetNMTokensFromPreviousAttempts(IList<NMToken> nmTokens)
		{
			MaybeInitBuilder();
			if (nmTokens == null || nmTokens.IsEmpty())
			{
				if (this.nmTokens != null)
				{
					this.nmTokens.Clear();
				}
				builder.ClearNmTokensFromPreviousAttempts();
				return;
			}
			this.nmTokens = new AList<NMToken>();
			Sharpen.Collections.AddAll(this.nmTokens, nmTokens);
		}

		private void InitLocalNewNMTokenList()
		{
			lock (this)
			{
				YarnServiceProtos.RegisterApplicationMasterResponseProtoOrBuilder p = viaProto ? 
					proto : builder;
				IList<YarnServiceProtos.NMTokenProto> list = p.GetNmTokensFromPreviousAttemptsList
					();
				nmTokens = new AList<NMToken>();
				foreach (YarnServiceProtos.NMTokenProto t in list)
				{
					nmTokens.AddItem(ConvertFromProtoFormat(t));
				}
			}
		}

		private IEnumerable<YarnServiceProtos.NMTokenProto> GetTokenProtoIterable(IList<NMToken
			> nmTokenList)
		{
			lock (this)
			{
				MaybeInitBuilder();
				return new _IEnumerable_343(this, nmTokenList);
			}
		}

		private sealed class _IEnumerable_343 : IEnumerable<YarnServiceProtos.NMTokenProto
			>
		{
			public _IEnumerable_343(RegisterApplicationMasterResponsePBImpl _enclosing, IList
				<NMToken> nmTokenList)
			{
				this._enclosing = _enclosing;
				this.nmTokenList = nmTokenList;
			}

			public override IEnumerator<YarnServiceProtos.NMTokenProto> GetEnumerator()
			{
				lock (this)
				{
					return new _IEnumerator_346(this, nmTokenList);
				}
			}

			private sealed class _IEnumerator_346 : IEnumerator<YarnServiceProtos.NMTokenProto
				>
			{
				public _IEnumerator_346(_IEnumerable_343 _enclosing, IList<NMToken> nmTokenList)
				{
					this._enclosing = _enclosing;
					this.nmTokenList = nmTokenList;
					this.iter = nmTokenList.GetEnumerator();
				}

				internal IEnumerator<NMToken> iter;

				public override bool HasNext()
				{
					return this.iter.HasNext();
				}

				public override YarnServiceProtos.NMTokenProto Next()
				{
					return this._enclosing._enclosing.ConvertToProtoFormat(this.iter.Next());
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				private readonly _IEnumerable_343 _enclosing;

				private readonly IList<NMToken> nmTokenList;
			}

			private readonly RegisterApplicationMasterResponsePBImpl _enclosing;

			private readonly IList<NMToken> nmTokenList;
		}

		public override EnumSet<YarnServiceProtos.SchedulerResourceTypes> GetSchedulerResourceTypes
			()
		{
			InitSchedulerResourceTypes();
			return this.schedulerResourceTypes;
		}

		private void InitSchedulerResourceTypes()
		{
			if (this.schedulerResourceTypes != null)
			{
				return;
			}
			YarnServiceProtos.RegisterApplicationMasterResponseProtoOrBuilder p = viaProto ? 
				proto : builder;
			IList<YarnServiceProtos.SchedulerResourceTypes> list = p.GetSchedulerResourceTypesList
				();
			if (list.IsEmpty())
			{
				this.schedulerResourceTypes = EnumSet.NoneOf<YarnServiceProtos.SchedulerResourceTypes
					>();
			}
			else
			{
				this.schedulerResourceTypes = EnumSet.CopyOf(list);
			}
		}

		private void AddSchedulerResourceTypes()
		{
			MaybeInitBuilder();
			builder.ClearSchedulerResourceTypes();
			if (schedulerResourceTypes == null)
			{
				return;
			}
			IEnumerable<YarnServiceProtos.SchedulerResourceTypes> values = new _IEnumerable_398
				(this);
			this.builder.AddAllSchedulerResourceTypes(values);
		}

		private sealed class _IEnumerable_398 : IEnumerable<YarnServiceProtos.SchedulerResourceTypes
			>
		{
			public _IEnumerable_398(RegisterApplicationMasterResponsePBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnServiceProtos.SchedulerResourceTypes> GetEnumerator
				()
			{
				return new _IEnumerator_402(this);
			}

			private sealed class _IEnumerator_402 : IEnumerator<YarnServiceProtos.SchedulerResourceTypes
				>
			{
				public _IEnumerator_402()
				{
					this.settingsIterator = this._enclosing._enclosing.schedulerResourceTypes.GetEnumerator
						();
				}

				internal IEnumerator<YarnServiceProtos.SchedulerResourceTypes> settingsIterator;

				public override bool HasNext()
				{
					return this.settingsIterator.HasNext();
				}

				public override YarnServiceProtos.SchedulerResourceTypes Next()
				{
					return this.settingsIterator.Next();
				}

				public override void Remove()
				{
					throw new NotSupportedException();
				}
			}

			private readonly RegisterApplicationMasterResponsePBImpl _enclosing;
		}

		public override void SetSchedulerResourceTypes(EnumSet<YarnServiceProtos.SchedulerResourceTypes
			> types)
		{
			if (types == null)
			{
				return;
			}
			InitSchedulerResourceTypes();
			this.schedulerResourceTypes.Clear();
			Sharpen.Collections.AddAll(this.schedulerResourceTypes, types);
		}

		private Resource ConvertFromProtoFormat(YarnProtos.ResourceProto resource)
		{
			return new ResourcePBImpl(resource);
		}

		private YarnProtos.ResourceProto ConvertToProtoFormat(Resource resource)
		{
			return ((ResourcePBImpl)resource).GetProto();
		}

		private ContainerPBImpl ConvertFromProtoFormat(YarnProtos.ContainerProto p)
		{
			return new ContainerPBImpl(p);
		}

		private YarnProtos.ContainerProto ConvertToProtoFormat(Container t)
		{
			return ((ContainerPBImpl)t).GetProto();
		}

		private YarnServiceProtos.NMTokenProto ConvertToProtoFormat(NMToken token)
		{
			return ((NMTokenPBImpl)token).GetProto();
		}

		private NMToken ConvertFromProtoFormat(YarnServiceProtos.NMTokenProto proto)
		{
			return new NMTokenPBImpl(proto);
		}
	}
}
