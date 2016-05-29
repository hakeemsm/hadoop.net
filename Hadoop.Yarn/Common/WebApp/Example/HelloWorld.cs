using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.Example
{
	/// <summary>The obligatory example.</summary>
	/// <remarks>
	/// The obligatory example. No xml/jsp/templates/config files! No
	/// proliferation of strange annotations either :)
	/// <p>3 in 1 example. Check results at
	/// <br />http://localhost:8888/hello and
	/// <br />http://localhost:8888/hello/html
	/// <br />http://localhost:8888/hello/json
	/// </remarks>
	public class HelloWorld
	{
		public class Hello : Controller
		{
			public override void Index()
			{
				RenderText("Hello world!");
			}

			public virtual void Html()
			{
				SetTitle("Hello world!");
			}

			public virtual void Json()
			{
				RenderJSON("Hello world!");
			}
		}

		public class HelloView : HtmlPage
		{
			protected internal override void Render(Hamlet.HTML<HtmlPage._> html)
			{
				html.Title($("title")).P("#hello-for-css").($("title")).().();
			}
			// produces valid html 4.01 strict
		}

		public static void Main(string[] args)
		{
			WebApps.$for(new HelloWorld()).At(8888).InDevMode().Start().JoinThread();
		}
	}
}
