using System.IO;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestJobQueueClient
	{
		/// <summary>Test that print job queue recursively prints child queues</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPrintJobQueueInfo()
		{
			JobQueueClient queueClient = new JobQueueClient();
			JobQueueInfo parent = new JobQueueInfo();
			JobQueueInfo child = new JobQueueInfo();
			JobQueueInfo grandChild = new JobQueueInfo();
			child.AddChild(grandChild);
			parent.AddChild(child);
			grandChild.SetQueueName("GrandChildQueue");
			ByteArrayOutputStream bbos = new ByteArrayOutputStream();
			PrintWriter writer = new PrintWriter(bbos);
			queueClient.PrintJobQueueInfo(parent, writer);
			NUnit.Framework.Assert.IsTrue("printJobQueueInfo did not print grandchild's name"
				, bbos.ToString().Contains("GrandChildQueue"));
		}
	}
}
