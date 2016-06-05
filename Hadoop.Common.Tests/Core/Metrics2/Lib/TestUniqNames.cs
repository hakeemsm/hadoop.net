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


namespace Org.Apache.Hadoop.Metrics2.Lib
{
	public class TestUniqNames
	{
		[Fact]
		public virtual void TestCommonCases()
		{
			UniqueNames u = new UniqueNames();
			Assert.Equal("foo", u.UniqueName("foo"));
			Assert.Equal("foo-1", u.UniqueName("foo"));
		}

		[Fact]
		public virtual void TestCollisions()
		{
			UniqueNames u = new UniqueNames();
			u.UniqueName("foo");
			Assert.Equal("foo-1", u.UniqueName("foo-1"));
			Assert.Equal("foo-2", u.UniqueName("foo"));
			Assert.Equal("foo-1-1", u.UniqueName("foo-1"));
			Assert.Equal("foo-2-1", u.UniqueName("foo-2"));
		}
	}
}
