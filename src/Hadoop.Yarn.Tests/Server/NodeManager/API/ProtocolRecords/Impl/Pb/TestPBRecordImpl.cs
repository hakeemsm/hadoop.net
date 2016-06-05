using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords.Impl.PB
{
	public class TestPBRecordImpl
	{
		internal static readonly RecordFactory recordFactory = CreatePBRecordFactory();

		internal static RecordFactory CreatePBRecordFactory()
		{
			Configuration conf = new Configuration();
			return RecordFactoryProvider.GetRecordFactory(conf);
		}

		internal static LocalResource CreateResource()
		{
			LocalResource ret = recordFactory.NewRecordInstance<LocalResource>();
			NUnit.Framework.Assert.IsTrue(ret is LocalResourcePBImpl);
			ret.SetResource(ConverterUtils.GetYarnUrlFromPath(new Path("hdfs://y.ak:8020/foo/bar"
				)));
			ret.SetSize(4344L);
			ret.SetTimestamp(3141592653589793L);
			ret.SetVisibility(LocalResourceVisibility.Public);
			return ret;
		}

		internal static LocalResourceStatus CreateLocalResourceStatus()
		{
			LocalResourceStatus ret = recordFactory.NewRecordInstance<LocalResourceStatus>();
			NUnit.Framework.Assert.IsTrue(ret is LocalResourceStatusPBImpl);
			ret.SetResource(CreateResource());
			ret.SetLocalPath(ConverterUtils.GetYarnUrlFromPath(new Path("file:///local/foo/bar"
				)));
			ret.SetStatus(ResourceStatusType.FetchSuccess);
			ret.SetLocalSize(4443L);
			Exception e = new Exception("Dingos.");
			e.SetStackTrace(new StackTraceElement[] { new StackTraceElement("foo", "bar", "baz"
				, 10), new StackTraceElement("sbb", "one", "onm", 10) });
			ret.SetException(SerializedException.NewInstance(e));
			return ret;
		}

		internal static LocalizerStatus CreateLocalizerStatus()
		{
			LocalizerStatus ret = recordFactory.NewRecordInstance<LocalizerStatus>();
			NUnit.Framework.Assert.IsTrue(ret is LocalizerStatusPBImpl);
			ret.SetLocalizerId("localizer0");
			ret.AddResourceStatus(CreateLocalResourceStatus());
			return ret;
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		internal static LocalizerHeartbeatResponse CreateLocalizerHeartbeatResponse()
		{
			LocalizerHeartbeatResponse ret = recordFactory.NewRecordInstance<LocalizerHeartbeatResponse
				>();
			NUnit.Framework.Assert.IsTrue(ret is LocalizerHeartbeatResponsePBImpl);
			ret.SetLocalizerAction(LocalizerAction.Live);
			LocalResource rsrc = CreateResource();
			AList<ResourceLocalizationSpec> rsrcs = new AList<ResourceLocalizationSpec>();
			ResourceLocalizationSpec resource = recordFactory.NewRecordInstance<ResourceLocalizationSpec
				>();
			resource.SetResource(rsrc);
			resource.SetDestinationDirectory(ConverterUtils.GetYarnUrlFromPath(new Path("/tmp"
				 + Runtime.CurrentTimeMillis())));
			rsrcs.AddItem(resource);
			ret.SetResourceSpecs(rsrcs);
			System.Console.Out.WriteLine(resource);
			return ret;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLocalResourceStatusSerDe()
		{
			LocalResourceStatus rsrcS = CreateLocalResourceStatus();
			NUnit.Framework.Assert.IsTrue(rsrcS is LocalResourceStatusPBImpl);
			LocalResourceStatusPBImpl rsrcPb = (LocalResourceStatusPBImpl)rsrcS;
			DataOutputBuffer @out = new DataOutputBuffer();
			rsrcPb.GetProto().WriteDelimitedTo(@out);
			DataInputBuffer @in = new DataInputBuffer();
			@in.Reset(@out.GetData(), 0, @out.GetLength());
			YarnServerNodemanagerServiceProtos.LocalResourceStatusProto rsrcPbD = YarnServerNodemanagerServiceProtos.LocalResourceStatusProto
				.ParseDelimitedFrom(@in);
			NUnit.Framework.Assert.IsNotNull(rsrcPbD);
			LocalResourceStatus rsrcD = new LocalResourceStatusPBImpl(rsrcPbD);
			NUnit.Framework.Assert.AreEqual(rsrcS, rsrcD);
			NUnit.Framework.Assert.AreEqual(CreateResource(), rsrcS.GetResource());
			NUnit.Framework.Assert.AreEqual(CreateResource(), rsrcD.GetResource());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLocalizerStatusSerDe()
		{
			LocalizerStatus rsrcS = CreateLocalizerStatus();
			NUnit.Framework.Assert.IsTrue(rsrcS is LocalizerStatusPBImpl);
			LocalizerStatusPBImpl rsrcPb = (LocalizerStatusPBImpl)rsrcS;
			DataOutputBuffer @out = new DataOutputBuffer();
			rsrcPb.GetProto().WriteDelimitedTo(@out);
			DataInputBuffer @in = new DataInputBuffer();
			@in.Reset(@out.GetData(), 0, @out.GetLength());
			YarnServerNodemanagerServiceProtos.LocalizerStatusProto rsrcPbD = YarnServerNodemanagerServiceProtos.LocalizerStatusProto
				.ParseDelimitedFrom(@in);
			NUnit.Framework.Assert.IsNotNull(rsrcPbD);
			LocalizerStatus rsrcD = new LocalizerStatusPBImpl(rsrcPbD);
			NUnit.Framework.Assert.AreEqual(rsrcS, rsrcD);
			NUnit.Framework.Assert.AreEqual("localizer0", rsrcS.GetLocalizerId());
			NUnit.Framework.Assert.AreEqual("localizer0", rsrcD.GetLocalizerId());
			NUnit.Framework.Assert.AreEqual(CreateLocalResourceStatus(), rsrcS.GetResourceStatus
				(0));
			NUnit.Framework.Assert.AreEqual(CreateLocalResourceStatus(), rsrcD.GetResourceStatus
				(0));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestLocalizerHeartbeatResponseSerDe()
		{
			LocalizerHeartbeatResponse rsrcS = CreateLocalizerHeartbeatResponse();
			NUnit.Framework.Assert.IsTrue(rsrcS is LocalizerHeartbeatResponsePBImpl);
			LocalizerHeartbeatResponsePBImpl rsrcPb = (LocalizerHeartbeatResponsePBImpl)rsrcS;
			DataOutputBuffer @out = new DataOutputBuffer();
			rsrcPb.GetProto().WriteDelimitedTo(@out);
			DataInputBuffer @in = new DataInputBuffer();
			@in.Reset(@out.GetData(), 0, @out.GetLength());
			YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto rsrcPbD = YarnServerNodemanagerServiceProtos.LocalizerHeartbeatResponseProto
				.ParseDelimitedFrom(@in);
			NUnit.Framework.Assert.IsNotNull(rsrcPbD);
			LocalizerHeartbeatResponse rsrcD = new LocalizerHeartbeatResponsePBImpl(rsrcPbD);
			NUnit.Framework.Assert.AreEqual(rsrcS, rsrcD);
			NUnit.Framework.Assert.AreEqual(CreateResource(), rsrcS.GetResourceSpecs()[0].GetResource
				());
			NUnit.Framework.Assert.AreEqual(CreateResource(), rsrcD.GetResourceSpecs()[0].GetResource
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSerializedExceptionDeSer()
		{
			// without cause
			YarnException yarnEx = new YarnException("Yarn_Exception");
			SerializedException serEx = SerializedException.NewInstance(yarnEx);
			Exception throwable = serEx.DeSerialize();
			NUnit.Framework.Assert.AreEqual(yarnEx.GetType(), throwable.GetType());
			NUnit.Framework.Assert.AreEqual(yarnEx.Message, throwable.Message);
			// with cause
			IOException ioe = new IOException("Test_IOException");
			RuntimeException runtimeException = new RuntimeException("Test_RuntimeException", 
				ioe);
			YarnException yarnEx2 = new YarnException("Test_YarnException", runtimeException);
			SerializedException serEx2 = SerializedException.NewInstance(yarnEx2);
			Exception throwable2 = serEx2.DeSerialize();
			Sharpen.Runtime.PrintStackTrace(throwable2);
			NUnit.Framework.Assert.AreEqual(yarnEx2.GetType(), throwable2.GetType());
			NUnit.Framework.Assert.AreEqual(yarnEx2.Message, throwable2.Message);
			NUnit.Framework.Assert.AreEqual(runtimeException.GetType(), throwable2.InnerException
				.GetType());
			NUnit.Framework.Assert.AreEqual(runtimeException.Message, throwable2.InnerException
				.Message);
			NUnit.Framework.Assert.AreEqual(ioe.GetType(), throwable2.InnerException.InnerException
				.GetType());
			NUnit.Framework.Assert.AreEqual(ioe.Message, throwable2.InnerException.InnerException
				.Message);
		}
	}
}
