using System;
using System.IO;
using Com.Google.Inject;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Webapp.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	/// <summary>
	/// This tests the NodesPage block table that it should contain the table body
	/// data for all the columns in the table as specified in the header.
	/// </summary>
	public class TestNodesPage
	{
		internal readonly int numberOfRacks = 2;

		internal readonly int numberOfNodesPerRack = 6;

		internal readonly int numberOfLostNodesPerRack = numberOfNodesPerRack / NodeState
			.Values().Length;

		internal readonly int numberOfThInMetricsTable = 20;

		internal readonly int numberOfActualTableHeaders = 13;

		private Injector injector;

		// The following is because of the way TestRMWebApp.mockRMContext creates
		// nodes.
		// Number of Actual Table Headers for NodesPage.NodesBlock might change in
		// future. In that case this value should be adjusted to the new value.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			RMContext mockRMContext = TestRMWebApp.MockRMContext(3, numberOfRacks, numberOfNodesPerRack
				, 8 * TestRMWebApp.GiB);
			injector = WebAppTests.CreateMockInjector<RMContext>(mockRMContext, new _Module_63
				(mockRMContext));
		}

		private sealed class _Module_63 : Module
		{
			public _Module_63(RMContext mockRMContext)
			{
				this.mockRMContext = mockRMContext;
			}

			public void Configure(Binder binder)
			{
				try
				{
					binder.Bind<ResourceManager>().ToInstance(TestRMWebApp.MockRm(mockRMContext));
				}
				catch (IOException e)
				{
					throw new InvalidOperationException(e);
				}
			}

			private readonly RMContext mockRMContext;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodesBlockRender()
		{
			injector.GetInstance<NodesPage.NodesBlock>().Render();
			PrintWriter writer = injector.GetInstance<PrintWriter>();
			WebAppTests.FlushOutput(injector);
			Org.Mockito.Mockito.Verify(writer, Org.Mockito.Mockito.Times(numberOfActualTableHeaders
				 + numberOfThInMetricsTable)).Write("<th");
			Org.Mockito.Mockito.Verify(writer, Org.Mockito.Mockito.Times(numberOfRacks * numberOfNodesPerRack
				 * numberOfActualTableHeaders + numberOfThInMetricsTable)).Write("<td");
		}

		[NUnit.Framework.Test]
		public virtual void TestNodesBlockRenderForLostNodes()
		{
			NodesPage.NodesBlock nodesBlock = injector.GetInstance<NodesPage.NodesBlock>();
			nodesBlock.Set("node.state", "lost");
			nodesBlock.Render();
			PrintWriter writer = injector.GetInstance<PrintWriter>();
			WebAppTests.FlushOutput(injector);
			Org.Mockito.Mockito.Verify(writer, Org.Mockito.Mockito.Times(numberOfActualTableHeaders
				 + numberOfThInMetricsTable)).Write("<th");
			Org.Mockito.Mockito.Verify(writer, Org.Mockito.Mockito.Times(numberOfRacks * numberOfLostNodesPerRack
				 * numberOfActualTableHeaders + numberOfThInMetricsTable)).Write("<td");
		}

		[NUnit.Framework.Test]
		public virtual void TestNodesBlockRenderForNodeLabelFilterWithNonEmptyLabel()
		{
			NodesPage.NodesBlock nodesBlock = injector.GetInstance<NodesPage.NodesBlock>();
			nodesBlock.Set("node.label", "x");
			nodesBlock.Render();
			PrintWriter writer = injector.GetInstance<PrintWriter>();
			WebAppTests.FlushOutput(injector);
			Org.Mockito.Mockito.Verify(writer, Org.Mockito.Mockito.Times(numberOfRacks * numberOfActualTableHeaders
				 + numberOfThInMetricsTable)).Write("<td");
		}

		[NUnit.Framework.Test]
		public virtual void TestNodesBlockRenderForNodeLabelFilterWithEmptyLabel()
		{
			NodesPage.NodesBlock nodesBlock = injector.GetInstance<NodesPage.NodesBlock>();
			nodesBlock.Set("node.label", string.Empty);
			nodesBlock.Render();
			PrintWriter writer = injector.GetInstance<PrintWriter>();
			WebAppTests.FlushOutput(injector);
			Org.Mockito.Mockito.Verify(writer, Org.Mockito.Mockito.Times(numberOfRacks * (numberOfNodesPerRack
				 - 1) * numberOfActualTableHeaders + numberOfThInMetricsTable)).Write("<td");
		}

		[NUnit.Framework.Test]
		public virtual void TestNodesBlockRenderForNodeLabelFilterWithAnyLabel()
		{
			NodesPage.NodesBlock nodesBlock = injector.GetInstance<NodesPage.NodesBlock>();
			nodesBlock.Set("node.label", "*");
			nodesBlock.Render();
			PrintWriter writer = injector.GetInstance<PrintWriter>();
			WebAppTests.FlushOutput(injector);
			Org.Mockito.Mockito.Verify(writer, Org.Mockito.Mockito.Times(numberOfRacks * numberOfNodesPerRack
				 * numberOfActualTableHeaders + numberOfThInMetricsTable)).Write("<td");
		}
	}
}
