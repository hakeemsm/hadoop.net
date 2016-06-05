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
using NUnit.Framework;
using NUnit.Framework.Runners;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util.Resource
{
	public class TestResourceCalculator
	{
		private ResourceCalculator resourceCalculator;

		[Parameterized.Parameters]
		public static ICollection<ResourceCalculator[]> GetParameters()
		{
			return Arrays.AsList(new ResourceCalculator[][] { new ResourceCalculator[] { new 
				DefaultResourceCalculator() }, new ResourceCalculator[] { new DominantResourceCalculator
				() } });
		}

		public TestResourceCalculator(ResourceCalculator rs)
		{
			this.resourceCalculator = rs;
		}

		public virtual void TestResourceCalculatorCompareMethod()
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(0, 0);
			// For lhs == rhs
			Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(0, 0);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource rhs = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(0, 0);
			AssertResourcesOperations(clusterResource, lhs, rhs, false, true, false, true, lhs
				, lhs);
			// lhs > rhs
			lhs = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(1, 1);
			rhs = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(0, 0);
			AssertResourcesOperations(clusterResource, lhs, rhs, false, false, true, true, lhs
				, rhs);
			// For lhs < rhs
			lhs = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(0, 0);
			rhs = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(1, 1);
			AssertResourcesOperations(clusterResource, lhs, rhs, true, true, false, false, rhs
				, lhs);
			if (!(resourceCalculator is DominantResourceCalculator))
			{
				return;
			}
			// verify for 2 dimensional resources i.e memory and cpu
			// dominant resource types
			lhs = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(1, 0);
			rhs = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(0, 1);
			AssertResourcesOperations(clusterResource, lhs, rhs, false, true, false, true, lhs
				, lhs);
			lhs = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(0, 1);
			rhs = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(1, 0);
			AssertResourcesOperations(clusterResource, lhs, rhs, false, true, false, true, lhs
				, lhs);
			lhs = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(1, 1);
			rhs = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(1, 0);
			AssertResourcesOperations(clusterResource, lhs, rhs, false, false, true, true, lhs
				, rhs);
			lhs = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(0, 1);
			rhs = Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(1, 1);
			AssertResourcesOperations(clusterResource, lhs, rhs, true, true, false, false, rhs
				, lhs);
		}

		private void AssertResourcesOperations(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource lhs, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 rhs, bool lessThan, bool lessThanOrEqual, bool greaterThan, bool greaterThanOrEqual
			, Org.Apache.Hadoop.Yarn.Api.Records.Resource max, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 min)
		{
			NUnit.Framework.Assert.AreEqual("Less Than operation is wrongly calculated.", lessThan
				, Resources.LessThan(resourceCalculator, clusterResource, lhs, rhs));
			NUnit.Framework.Assert.AreEqual("Less Than Or Equal To operation is wrongly calculated."
				, lessThanOrEqual, Resources.LessThanOrEqual(resourceCalculator, clusterResource
				, lhs, rhs));
			NUnit.Framework.Assert.AreEqual("Greater Than operation is wrongly calculated.", 
				greaterThan, Resources.GreaterThan(resourceCalculator, clusterResource, lhs, rhs
				));
			NUnit.Framework.Assert.AreEqual("Greater Than Or Equal To operation is wrongly calculated."
				, greaterThanOrEqual, Resources.GreaterThanOrEqual(resourceCalculator, clusterResource
				, lhs, rhs));
			NUnit.Framework.Assert.AreEqual("Max(value) Operation wrongly calculated.", max, 
				Resources.Max(resourceCalculator, clusterResource, lhs, rhs));
			NUnit.Framework.Assert.AreEqual("Min(value) operation is wrongly calculated.", min
				, Resources.Min(resourceCalculator, clusterResource, lhs, rhs));
		}
	}
}
