/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System.Collections.Generic;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestRMHAForNodeLabels : RMHATestBase
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestSubmitApplicationWithRMHA
			));

		/// <exception cref="System.Exception"/>
		[SetUp]
		public override void Setup()
		{
			base.Setup();
			// Create directory for node label store 
			FilePath tempDir = FilePath.CreateTempFile("nlb", ".tmp");
			tempDir.Delete();
			tempDir.Mkdirs();
			tempDir.DeleteOnExit();
			confForRM1.SetBoolean(YarnConfiguration.NodeLabelsEnabled, true);
			confForRM1.Set(YarnConfiguration.FsNodeLabelsStoreRootDir, tempDir.GetAbsolutePath
				());
			confForRM2.SetBoolean(YarnConfiguration.NodeLabelsEnabled, true);
			confForRM2.Set(YarnConfiguration.FsNodeLabelsStoreRootDir, tempDir.GetAbsolutePath
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMHARecoverNodeLabels()
		{
			// start two RMs, and transit rm1 to active, rm2 to standby
			StartRMs();
			// Add labels to rm1
			rm1.GetRMContext().GetNodeLabelManager().AddToCluserNodeLabels(ImmutableSet.Of("a"
				, "b", "c"));
			IDictionary<NodeId, ICollection<string>> nodeToLabels = new Dictionary<NodeId, ICollection
				<string>>();
			nodeToLabels[NodeId.NewInstance("host1", 0)] = ImmutableSet.Of("a");
			nodeToLabels[NodeId.NewInstance("host2", 0)] = ImmutableSet.Of("b");
			rm1.GetRMContext().GetNodeLabelManager().ReplaceLabelsOnNode(nodeToLabels);
			// Do the failover
			ExplicitFailover();
			// Check labels in rm2
			NUnit.Framework.Assert.IsTrue(rm2.GetRMContext().GetNodeLabelManager().GetClusterNodeLabels
				().ContainsAll(ImmutableSet.Of("a", "b", "c")));
			NUnit.Framework.Assert.IsTrue(rm2.GetRMContext().GetNodeLabelManager().GetNodeLabels
				()[NodeId.NewInstance("host1", 0)].Contains("a"));
			NUnit.Framework.Assert.IsTrue(rm2.GetRMContext().GetNodeLabelManager().GetNodeLabels
				()[NodeId.NewInstance("host2", 0)].Contains("b"));
		}
	}
}
