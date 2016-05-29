using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	public class TestTwoColumnLayout
	{
		public class TestController : Controller
		{
			public override void Index()
			{
				SetTitle("Test the two column table layout");
				Set("ui.accordion.id", "nav");
				Render(typeof(TwoColumnLayout));
			}
		}

		[NUnit.Framework.Test]
		public virtual void ShouldNotThrow()
		{
			WebAppTests.TestPage(typeof(TwoColumnLayout));
		}

		public static void Main(string[] args)
		{
			WebApps.$for("test").At(8888).InDevMode().Start().JoinThread();
		}
	}
}
