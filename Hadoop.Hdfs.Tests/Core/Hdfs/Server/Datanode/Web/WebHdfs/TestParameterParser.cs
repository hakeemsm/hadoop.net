using System;
using IO.Netty.Handler.Codec.Http;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Web.Webhdfs
{
	public class TestParameterParser
	{
		private const string LogicalName = "minidfs";

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDeserializeHAToken()
		{
			Configuration conf = DFSTestUtil.NewHAConfiguration(LogicalName);
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>();
			QueryStringDecoder decoder = new QueryStringDecoder(WebHdfsHandler.WebhdfsPrefix 
				+ "/?" + NamenodeAddressParam.Name + "=" + LogicalName + "&" + DelegationParam.Name
				 + "=" + token.EncodeToUrlString());
			ParameterParser testParser = new ParameterParser(decoder, conf);
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> tok2 = testParser
				.DelegationToken();
			NUnit.Framework.Assert.IsTrue(HAUtil.IsTokenForLogicalUri(tok2));
		}

		[NUnit.Framework.Test]
		public virtual void TestDecodePath()
		{
			string EscapedPath = "/test%25+1%26%3Dtest?op=OPEN&foo=bar";
			string ExpectedPath = "/test%+1&=test";
			Configuration conf = new Configuration();
			QueryStringDecoder decoder = new QueryStringDecoder(WebHdfsHandler.WebhdfsPrefix 
				+ EscapedPath);
			ParameterParser testParser = new ParameterParser(decoder, conf);
			NUnit.Framework.Assert.AreEqual(ExpectedPath, testParser.Path());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestOffset()
		{
			long X = 42;
			long offset = new OffsetParam(System.Convert.ToString(X)).GetOffset();
			NUnit.Framework.Assert.AreEqual("OffsetParam: ", X, offset);
			offset = new OffsetParam((string)null).GetOffset();
			NUnit.Framework.Assert.AreEqual("OffsetParam with null should have defaulted to 0"
				, 0, offset);
			try
			{
				offset = new OffsetParam("abc").GetValue();
				NUnit.Framework.Assert.Fail("OffsetParam with nondigit value should have thrown IllegalArgumentException"
					);
			}
			catch (ArgumentException)
			{
			}
		}
		// Ignore
	}
}
