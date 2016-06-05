using System;
using System.Collections.Generic;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Proto;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB
{
	public class ContainerLaunchContextPBImpl : ContainerLaunchContext
	{
		internal YarnProtos.ContainerLaunchContextProto proto = YarnProtos.ContainerLaunchContextProto
			.GetDefaultInstance();

		internal YarnProtos.ContainerLaunchContextProto.Builder builder = null;

		internal bool viaProto = false;

		private IDictionary<string, LocalResource> localResources = null;

		private ByteBuffer tokens = null;

		private IDictionary<string, ByteBuffer> serviceData = null;

		private IDictionary<string, string> environment = null;

		private IList<string> commands = null;

		private IDictionary<ApplicationAccessType, string> applicationACLS = null;

		public ContainerLaunchContextPBImpl()
		{
			builder = YarnProtos.ContainerLaunchContextProto.NewBuilder();
		}

		public ContainerLaunchContextPBImpl(YarnProtos.ContainerLaunchContextProto proto)
		{
			this.proto = proto;
			viaProto = true;
		}

		public virtual YarnProtos.ContainerLaunchContextProto GetProto()
		{
			MergeLocalToProto();
			proto = viaProto ? proto : ((YarnProtos.ContainerLaunchContextProto)builder.Build
				());
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

		protected internal ByteBuffer ConvertFromProtoFormat(ByteString byteString)
		{
			return ProtoUtils.ConvertFromProtoFormat(byteString);
		}

		protected internal ByteString ConvertToProtoFormat(ByteBuffer byteBuffer)
		{
			return ProtoUtils.ConvertToProtoFormat(byteBuffer);
		}

		private void MergeLocalToBuilder()
		{
			if (this.localResources != null)
			{
				AddLocalResourcesToProto();
			}
			if (this.tokens != null)
			{
				builder.SetTokens(ConvertToProtoFormat(this.tokens));
			}
			if (this.serviceData != null)
			{
				AddServiceDataToProto();
			}
			if (this.environment != null)
			{
				AddEnvToProto();
			}
			if (this.commands != null)
			{
				AddCommandsToProto();
			}
			if (this.applicationACLS != null)
			{
				AddApplicationACLs();
			}
		}

		private void MergeLocalToProto()
		{
			if (viaProto)
			{
				MaybeInitBuilder();
			}
			MergeLocalToBuilder();
			proto = ((YarnProtos.ContainerLaunchContextProto)builder.Build());
			viaProto = true;
		}

		private void MaybeInitBuilder()
		{
			if (viaProto || builder == null)
			{
				builder = YarnProtos.ContainerLaunchContextProto.NewBuilder(proto);
			}
			viaProto = false;
		}

		public override IList<string> GetCommands()
		{
			InitCommands();
			return this.commands;
		}

		private void InitCommands()
		{
			if (this.commands != null)
			{
				return;
			}
			YarnProtos.ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
			IList<string> list = p.GetCommandList();
			this.commands = new AList<string>();
			foreach (string c in list)
			{
				this.commands.AddItem(c);
			}
		}

		public override void SetCommands(IList<string> commands)
		{
			if (commands == null)
			{
				return;
			}
			InitCommands();
			this.commands.Clear();
			Sharpen.Collections.AddAll(this.commands, commands);
		}

		private void AddCommandsToProto()
		{
			MaybeInitBuilder();
			builder.ClearCommand();
			if (this.commands == null)
			{
				return;
			}
			builder.AddAllCommand(this.commands);
		}

		public override IDictionary<string, LocalResource> GetLocalResources()
		{
			InitLocalResources();
			return this.localResources;
		}

		private void InitLocalResources()
		{
			if (this.localResources != null)
			{
				return;
			}
			YarnProtos.ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.StringLocalResourceMapProto> list = p.GetLocalResourcesList();
			this.localResources = new Dictionary<string, LocalResource>();
			foreach (YarnProtos.StringLocalResourceMapProto c in list)
			{
				this.localResources[c.GetKey()] = ConvertFromProtoFormat(c.GetValue());
			}
		}

		public override void SetLocalResources(IDictionary<string, LocalResource> localResources
			)
		{
			if (localResources == null)
			{
				return;
			}
			InitLocalResources();
			this.localResources.Clear();
			this.localResources.PutAll(localResources);
		}

		private void AddLocalResourcesToProto()
		{
			MaybeInitBuilder();
			builder.ClearLocalResources();
			if (localResources == null)
			{
				return;
			}
			IEnumerable<YarnProtos.StringLocalResourceMapProto> iterable = new _IEnumerable_211
				(this);
			builder.AddAllLocalResources(iterable);
		}

		private sealed class _IEnumerable_211 : IEnumerable<YarnProtos.StringLocalResourceMapProto
			>
		{
			public _IEnumerable_211(ContainerLaunchContextPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.StringLocalResourceMapProto> GetEnumerator
				()
			{
				return new _IEnumerator_215(this);
			}

			private sealed class _IEnumerator_215 : IEnumerator<YarnProtos.StringLocalResourceMapProto
				>
			{
				public _IEnumerator_215(_IEnumerable_211 _enclosing)
				{
					this._enclosing = _enclosing;
					this.keyIter = this._enclosing._enclosing.localResources.Keys.GetEnumerator();
				}

				internal IEnumerator<string> keyIter;

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				public override YarnProtos.StringLocalResourceMapProto Next()
				{
					string key = this.keyIter.Next();
					return ((YarnProtos.StringLocalResourceMapProto)YarnProtos.StringLocalResourceMapProto
						.NewBuilder().SetKey(key).SetValue(this._enclosing._enclosing.ConvertToProtoFormat
						(this._enclosing._enclosing.localResources[key])).Build());
				}

				public override bool HasNext()
				{
					return this.keyIter.HasNext();
				}

				private readonly _IEnumerable_211 _enclosing;
			}

			private readonly ContainerLaunchContextPBImpl _enclosing;
		}

		public override ByteBuffer GetTokens()
		{
			YarnProtos.ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
			if (this.tokens != null)
			{
				return this.tokens;
			}
			if (!p.HasTokens())
			{
				return null;
			}
			this.tokens = ConvertFromProtoFormat(p.GetTokens());
			return this.tokens;
		}

		public override void SetTokens(ByteBuffer tokens)
		{
			MaybeInitBuilder();
			if (tokens == null)
			{
				builder.ClearTokens();
			}
			this.tokens = tokens;
		}

		public override IDictionary<string, ByteBuffer> GetServiceData()
		{
			InitServiceData();
			return this.serviceData;
		}

		private void InitServiceData()
		{
			if (this.serviceData != null)
			{
				return;
			}
			YarnProtos.ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.StringBytesMapProto> list = p.GetServiceDataList();
			this.serviceData = new Dictionary<string, ByteBuffer>();
			foreach (YarnProtos.StringBytesMapProto c in list)
			{
				this.serviceData[c.GetKey()] = ConvertFromProtoFormat(c.GetValue());
			}
		}

		public override void SetServiceData(IDictionary<string, ByteBuffer> serviceData)
		{
			if (serviceData == null)
			{
				return;
			}
			InitServiceData();
			this.serviceData.PutAll(serviceData);
		}

		private void AddServiceDataToProto()
		{
			MaybeInitBuilder();
			builder.ClearServiceData();
			if (serviceData == null)
			{
				return;
			}
			IEnumerable<YarnProtos.StringBytesMapProto> iterable = new _IEnumerable_296(this);
			builder.AddAllServiceData(iterable);
		}

		private sealed class _IEnumerable_296 : IEnumerable<YarnProtos.StringBytesMapProto
			>
		{
			public _IEnumerable_296(ContainerLaunchContextPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.StringBytesMapProto> GetEnumerator()
			{
				return new _IEnumerator_300(this);
			}

			private sealed class _IEnumerator_300 : IEnumerator<YarnProtos.StringBytesMapProto
				>
			{
				public _IEnumerator_300(_IEnumerable_296 _enclosing)
				{
					this._enclosing = _enclosing;
					this.keyIter = this._enclosing._enclosing.serviceData.Keys.GetEnumerator();
				}

				internal IEnumerator<string> keyIter;

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				public override YarnProtos.StringBytesMapProto Next()
				{
					string key = this.keyIter.Next();
					return ((YarnProtos.StringBytesMapProto)YarnProtos.StringBytesMapProto.NewBuilder
						().SetKey(key).SetValue(this._enclosing._enclosing.ConvertToProtoFormat(this._enclosing
						._enclosing.serviceData[key])).Build());
				}

				public override bool HasNext()
				{
					return this.keyIter.HasNext();
				}

				private readonly _IEnumerable_296 _enclosing;
			}

			private readonly ContainerLaunchContextPBImpl _enclosing;
		}

		public override IDictionary<string, string> GetEnvironment()
		{
			InitEnv();
			return this.environment;
		}

		private void InitEnv()
		{
			if (this.environment != null)
			{
				return;
			}
			YarnProtos.ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
			IList<YarnProtos.StringStringMapProto> list = p.GetEnvironmentList();
			this.environment = new Dictionary<string, string>();
			foreach (YarnProtos.StringStringMapProto c in list)
			{
				this.environment[c.GetKey()] = c.GetValue();
			}
		}

		public override void SetEnvironment(IDictionary<string, string> env)
		{
			if (env == null)
			{
				return;
			}
			InitEnv();
			this.environment.Clear();
			this.environment.PutAll(env);
		}

		private void AddEnvToProto()
		{
			MaybeInitBuilder();
			builder.ClearEnvironment();
			if (environment == null)
			{
				return;
			}
			IEnumerable<YarnProtos.StringStringMapProto> iterable = new _IEnumerable_360(this
				);
			builder.AddAllEnvironment(iterable);
		}

		private sealed class _IEnumerable_360 : IEnumerable<YarnProtos.StringStringMapProto
			>
		{
			public _IEnumerable_360(ContainerLaunchContextPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.StringStringMapProto> GetEnumerator()
			{
				return new _IEnumerator_364(this);
			}

			private sealed class _IEnumerator_364 : IEnumerator<YarnProtos.StringStringMapProto
				>
			{
				public _IEnumerator_364(_IEnumerable_360 _enclosing)
				{
					this._enclosing = _enclosing;
					this.keyIter = this._enclosing._enclosing.environment.Keys.GetEnumerator();
				}

				internal IEnumerator<string> keyIter;

				public override void Remove()
				{
					throw new NotSupportedException();
				}

				public override YarnProtos.StringStringMapProto Next()
				{
					string key = this.keyIter.Next();
					return ((YarnProtos.StringStringMapProto)YarnProtos.StringStringMapProto.NewBuilder
						().SetKey(key).SetValue((this._enclosing._enclosing.environment[key])).Build());
				}

				public override bool HasNext()
				{
					return this.keyIter.HasNext();
				}

				private readonly _IEnumerable_360 _enclosing;
			}

			private readonly ContainerLaunchContextPBImpl _enclosing;
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
			YarnProtos.ContainerLaunchContextProtoOrBuilder p = viaProto ? proto : builder;
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
			IEnumerable<YarnProtos.ApplicationACLMapProto> values = new _IEnumerable_418(this
				);
			this.builder.AddAllApplicationACLs(values);
		}

		private sealed class _IEnumerable_418 : IEnumerable<YarnProtos.ApplicationACLMapProto
			>
		{
			public _IEnumerable_418(ContainerLaunchContextPBImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override IEnumerator<YarnProtos.ApplicationACLMapProto> GetEnumerator()
			{
				return new _IEnumerator_422(this);
			}

			private sealed class _IEnumerator_422 : IEnumerator<YarnProtos.ApplicationACLMapProto
				>
			{
				public _IEnumerator_422(_IEnumerable_418 _enclosing)
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

				private readonly _IEnumerable_418 _enclosing;
			}

			private readonly ContainerLaunchContextPBImpl _enclosing;
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

		private LocalResourcePBImpl ConvertFromProtoFormat(YarnProtos.LocalResourceProto 
			p)
		{
			return new LocalResourcePBImpl(p);
		}

		private YarnProtos.LocalResourceProto ConvertToProtoFormat(LocalResource t)
		{
			return ((LocalResourcePBImpl)t).GetProto();
		}
	}
}
