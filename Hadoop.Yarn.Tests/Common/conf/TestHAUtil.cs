using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Conf
{
	public class TestHAUtil
	{
		private Configuration conf;

		private const string Rm1AddressUntrimmed = "  \t\t\n 1.2.3.4:8021  \n\t ";

		private static readonly string Rm1Address = Rm1AddressUntrimmed.Trim();

		private const string Rm2Address = "localhost:8022";

		private const string Rm3Address = "localhost:8033";

		private const string Rm1NodeIdUntrimmed = "rm1 ";

		private static readonly string Rm1NodeId = Rm1NodeIdUntrimmed.Trim();

		private const string Rm2NodeId = "rm2";

		private const string Rm3NodeId = "rm3";

		private const string RmInvalidNodeId = ".rm";

		private const string RmNodeIdsUntrimmed = Rm1NodeIdUntrimmed + "," + Rm2NodeId;

		private static readonly string RmNodeIds = Rm1NodeId + "," + Rm2NodeId;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			conf.Set(YarnConfiguration.RmHaIds, RmNodeIdsUntrimmed);
			conf.Set(YarnConfiguration.RmHaId, Rm1NodeIdUntrimmed);
			foreach (string confKey in YarnConfiguration.GetServiceAddressConfKeys(conf))
			{
				// configuration key itself cannot contains space/tab/return chars.
				conf.Set(HAUtil.AddSuffix(confKey, Rm1NodeId), Rm1AddressUntrimmed);
				conf.Set(HAUtil.AddSuffix(confKey, Rm2NodeId), Rm2Address);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetRMServiceId()
		{
			conf.Set(YarnConfiguration.RmHaIds, Rm1NodeId + "," + Rm2NodeId);
			ICollection<string> rmhaIds = HAUtil.GetRMHAIds(conf);
			NUnit.Framework.Assert.AreEqual(2, rmhaIds.Count);
			string[] ids = Sharpen.Collections.ToArray(rmhaIds, new string[0]);
			NUnit.Framework.Assert.AreEqual(Rm1NodeId, ids[0]);
			NUnit.Framework.Assert.AreEqual(Rm2NodeId, ids[1]);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetRMId()
		{
			conf.Set(YarnConfiguration.RmHaId, Rm1NodeId);
			NUnit.Framework.Assert.AreEqual("Does not honor " + YarnConfiguration.RmHaId, Rm1NodeId
				, HAUtil.GetRMHAId(conf));
			conf.Clear();
			NUnit.Framework.Assert.IsNull("Return null when " + YarnConfiguration.RmHaId + " is not set"
				, HAUtil.GetRMHAId(conf));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVerifyAndSetConfiguration()
		{
			try
			{
				HAUtil.VerifyAndSetConfiguration(conf);
			}
			catch (YarnRuntimeException)
			{
				NUnit.Framework.Assert.Fail("Should not throw any exceptions.");
			}
			NUnit.Framework.Assert.AreEqual("Should be saved as Trimmed collection", StringUtils
				.GetStringCollection(RmNodeIds), HAUtil.GetRMHAIds(conf));
			NUnit.Framework.Assert.AreEqual("Should be saved as Trimmed string", Rm1NodeId, HAUtil
				.GetRMHAId(conf));
			foreach (string confKey in YarnConfiguration.GetServiceAddressConfKeys(conf))
			{
				NUnit.Framework.Assert.AreEqual("RPC address not set for " + confKey, Rm1Address, 
					conf.Get(confKey));
			}
			conf.Clear();
			conf.Set(YarnConfiguration.RmHaIds, Rm1NodeId);
			try
			{
				HAUtil.VerifyAndSetConfiguration(conf);
			}
			catch (YarnRuntimeException e)
			{
				NUnit.Framework.Assert.AreEqual("YarnRuntimeException by verifyAndSetRMHAIds()", 
					HAUtil.BadConfigMessagePrefix + HAUtil.GetInvalidValueMessage(YarnConfiguration.
					RmHaIds, conf.Get(YarnConfiguration.RmHaIds) + "\nHA mode requires atleast two RMs"
					), e.Message);
			}
			conf.Clear();
			// simulate the case YarnConfiguration.RM_HA_ID is not set
			conf.Set(YarnConfiguration.RmHaIds, Rm1NodeId + "," + Rm2NodeId);
			foreach (string confKey_1 in YarnConfiguration.GetServiceAddressConfKeys(conf))
			{
				conf.Set(HAUtil.AddSuffix(confKey_1, Rm1NodeId), Rm1Address);
				conf.Set(HAUtil.AddSuffix(confKey_1, Rm2NodeId), Rm2Address);
			}
			try
			{
				HAUtil.VerifyAndSetConfiguration(conf);
			}
			catch (YarnRuntimeException e)
			{
				NUnit.Framework.Assert.AreEqual("YarnRuntimeException by getRMId()", HAUtil.BadConfigMessagePrefix
					 + HAUtil.GetNeedToSetValueMessage(YarnConfiguration.RmHaId), e.Message);
			}
			conf.Clear();
			conf.Set(YarnConfiguration.RmHaId, RmInvalidNodeId);
			conf.Set(YarnConfiguration.RmHaIds, RmInvalidNodeId + "," + Rm1NodeId);
			foreach (string confKey_2 in YarnConfiguration.GetServiceAddressConfKeys(conf))
			{
				// simulate xml with invalid node id
				conf.Set(confKey_2 + RmInvalidNodeId, RmInvalidNodeId);
			}
			try
			{
				HAUtil.VerifyAndSetConfiguration(conf);
			}
			catch (YarnRuntimeException e)
			{
				NUnit.Framework.Assert.AreEqual("YarnRuntimeException by addSuffix()", HAUtil.BadConfigMessagePrefix
					 + HAUtil.GetInvalidValueMessage(YarnConfiguration.RmHaId, RmInvalidNodeId), e.Message
					);
			}
			conf.Clear();
			// simulate the case HAUtil.RM_RPC_ADDRESS_CONF_KEYS are not set
			conf.Set(YarnConfiguration.RmHaId, Rm1NodeId);
			conf.Set(YarnConfiguration.RmHaIds, Rm1NodeId + "," + Rm2NodeId);
			try
			{
				HAUtil.VerifyAndSetConfiguration(conf);
				NUnit.Framework.Assert.Fail("Should throw YarnRuntimeException. by Configuration#set()"
					);
			}
			catch (YarnRuntimeException e)
			{
				string confKey_3 = HAUtil.AddSuffix(YarnConfiguration.RmAddress, Rm1NodeId);
				NUnit.Framework.Assert.AreEqual("YarnRuntimeException by Configuration#set()", HAUtil
					.BadConfigMessagePrefix + HAUtil.GetNeedToSetValueMessage(HAUtil.AddSuffix(YarnConfiguration
					.RmHostname, Rm1NodeId) + " or " + confKey_3), e.Message);
			}
			// simulate the case YarnConfiguration.RM_HA_IDS doesn't contain
			// the value of YarnConfiguration.RM_HA_ID
			conf.Clear();
			conf.Set(YarnConfiguration.RmHaIds, Rm2NodeId + "," + Rm3NodeId);
			conf.Set(YarnConfiguration.RmHaId, Rm1NodeIdUntrimmed);
			foreach (string confKey_4 in YarnConfiguration.GetServiceAddressConfKeys(conf))
			{
				conf.Set(HAUtil.AddSuffix(confKey_4, Rm1NodeId), Rm1AddressUntrimmed);
				conf.Set(HAUtil.AddSuffix(confKey_4, Rm2NodeId), Rm2Address);
				conf.Set(HAUtil.AddSuffix(confKey_4, Rm3NodeId), Rm3Address);
			}
			try
			{
				HAUtil.VerifyAndSetConfiguration(conf);
			}
			catch (YarnRuntimeException e)
			{
				NUnit.Framework.Assert.AreEqual("YarnRuntimeException by getRMId()'s validation", 
					HAUtil.BadConfigMessagePrefix + HAUtil.GetRMHAIdNeedToBeIncludedMessage("[rm2, rm3]"
					, Rm1NodeId), e.Message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestGetConfKeyForRMInstance()
		{
			NUnit.Framework.Assert.IsTrue("RM instance id is not suffixed", HAUtil.GetConfKeyForRMInstance
				(YarnConfiguration.RmAddress, conf).Contains(HAUtil.GetRMHAId(conf)));
			NUnit.Framework.Assert.IsFalse("RM instance id is suffixed", HAUtil.GetConfKeyForRMInstance
				(YarnConfiguration.NmAddress, conf).Contains(HAUtil.GetRMHAId(conf)));
		}
	}
}
