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
using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Lib
{
	public class TestInterns
	{
		[NUnit.Framework.Test]
		public virtual void TestInfo()
		{
			MetricsInfo info = Interns.Info("m", "m desc");
			NUnit.Framework.Assert.AreSame("same info", info, Interns.Info("m", "m desc"));
		}

		[NUnit.Framework.Test]
		public virtual void TestTag()
		{
			MetricsTag tag = Interns.Tag("t", "t desc", "t value");
			NUnit.Framework.Assert.AreSame("same tag", tag, Interns.Tag("t", "t desc", "t value"
				));
		}

		[NUnit.Framework.Test]
		public virtual void TestInfoOverflow()
		{
			MetricsInfo i0 = Interns.Info("m0", "m desc");
			for (int i = 0; i < MaxInfoNames + 1; ++i)
			{
				Interns.Info("m" + i, "m desc");
				if (i < MaxInfoNames)
				{
					NUnit.Framework.Assert.AreSame("m0 is still there", i0, Interns.Info("m0", "m desc"
						));
				}
			}
			NUnit.Framework.Assert.AreNotSame("m0 is gone", i0, Interns.Info("m0", "m desc"));
			MetricsInfo i1 = Interns.Info("m1", "m desc");
			for (int i_1 = 0; i_1 < MaxInfoDescs; ++i_1)
			{
				Interns.Info("m1", "m desc" + i_1);
				if (i_1 < MaxInfoDescs - 1)
				{
					NUnit.Framework.Assert.AreSame("i1 is still there", i1, Interns.Info("m1", "m desc"
						));
				}
			}
			NUnit.Framework.Assert.AreNotSame("i1 is gone", i1, Interns.Info("m1", "m desc"));
		}

		[NUnit.Framework.Test]
		public virtual void TestTagOverflow()
		{
			MetricsTag t0 = Interns.Tag("t0", "t desc", "t value");
			for (int i = 0; i < MaxTagNames + 1; ++i)
			{
				Interns.Tag("t" + i, "t desc", "t value");
				if (i < MaxTagNames)
				{
					NUnit.Framework.Assert.AreSame("t0 still there", t0, Interns.Tag("t0", "t desc", 
						"t value"));
				}
			}
			NUnit.Framework.Assert.AreNotSame("t0 is gone", t0, Interns.Tag("t0", "t desc", "t value"
				));
			MetricsTag t1 = Interns.Tag("t1", "t desc", "t value");
			for (int i_1 = 0; i_1 < MaxTagValues; ++i_1)
			{
				Interns.Tag("t1", "t desc", "t value" + i_1);
				if (i_1 < MaxTagValues - 1)
				{
					NUnit.Framework.Assert.AreSame("t1 is still there", t1, Interns.Tag("t1", "t desc"
						, "t value"));
				}
			}
			NUnit.Framework.Assert.AreNotSame("t1 is gone", t1, Interns.Tag("t1", "t desc", "t value"
				));
		}
	}
}
