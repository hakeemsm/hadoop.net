/*
* TestGangliaContext.java
*
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
using Org.Apache.Hadoop.Metrics;
using Org.Apache.Hadoop.Metrics.Spi;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics.Ganglia
{
	public class TestGangliaContext
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestShouldCreateDatagramSocketByDefault()
		{
			GangliaContext context = new GangliaContext();
			context.Init("gangliaContext", ContextFactory.GetFactory());
			NUnit.Framework.Assert.IsFalse("Created MulticastSocket", context.datagramSocket 
				is MulticastSocket);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestShouldCreateDatagramSocketIfMulticastIsDisabled()
		{
			GangliaContext context = new GangliaContext();
			ContextFactory factory = ContextFactory.GetFactory();
			factory.SetAttribute("gangliaContext.multicast", "false");
			context.Init("gangliaContext", factory);
			NUnit.Framework.Assert.IsFalse("Created MulticastSocket", context.datagramSocket 
				is MulticastSocket);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestShouldCreateMulticastSocket()
		{
			GangliaContext context = new GangliaContext();
			ContextFactory factory = ContextFactory.GetFactory();
			factory.SetAttribute("gangliaContext.multicast", "true");
			context.Init("gangliaContext", factory);
			Assert.True("Did not create MulticastSocket", context.datagramSocket
				 is MulticastSocket);
			MulticastSocket multicastSocket = (MulticastSocket)context.datagramSocket;
			Assert.Equal("Did not set default TTL", multicastSocket.GetTimeToLive
				(), 1);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestShouldSetMulticastSocketTtl()
		{
			GangliaContext context = new GangliaContext();
			ContextFactory factory = ContextFactory.GetFactory();
			factory.SetAttribute("gangliaContext.multicast", "true");
			factory.SetAttribute("gangliaContext.multicast.ttl", "10");
			context.Init("gangliaContext", factory);
			MulticastSocket multicastSocket = (MulticastSocket)context.datagramSocket;
			Assert.Equal("Did not set TTL", multicastSocket.GetTimeToLive(
				), 10);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCloseShouldCloseTheSocketWhichIsCreatedByInit()
		{
			AbstractMetricsContext context = new GangliaContext();
			context.Init("gangliaContext", ContextFactory.GetFactory());
			GangliaContext gangliaContext = (GangliaContext)context;
			NUnit.Framework.Assert.IsFalse("Socket already closed", gangliaContext.datagramSocket
				.IsClosed());
			context.Close();
			Assert.True("Socket not closed", gangliaContext.datagramSocket.
				IsClosed());
		}
	}
}
