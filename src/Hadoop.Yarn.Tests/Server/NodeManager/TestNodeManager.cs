using System.IO;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class TestNodeManager
	{
		public sealed class InvalidContainerExecutor : DefaultContainerExecutor
		{
			/// <exception cref="System.IO.IOException"/>
			public override void Init()
			{
				throw new IOException("dummy executor init called");
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestContainerExecutorInitCall()
		{
			NodeManager nm = new NodeManager();
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetClass(YarnConfiguration.NmContainerExecutor, typeof(TestNodeManager.InvalidContainerExecutor
				), typeof(ContainerExecutor));
			try
			{
				nm.Init(conf);
				NUnit.Framework.Assert.Fail("Init should fail");
			}
			catch (YarnRuntimeException e)
			{
				//PASS
				System.Diagnostics.Debug.Assert((e.InnerException.Message.Contains("dummy executor init called"
					)));
			}
			finally
			{
				nm.Stop();
			}
		}
	}
}
