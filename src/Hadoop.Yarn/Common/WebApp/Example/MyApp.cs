using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.Example
{
	/// <summary>
	/// The embedded UI serves two pages at:
	/// <br />http://localhost:8888/my and
	/// <br />http://localhost:8888/my/anythingYouWant
	/// </summary>
	public class MyApp
	{
		// This is an app API
		public virtual string AnyAPI()
		{
			return "anything, really!";
		}

		public class MyController : Controller
		{
			internal readonly MyApp app;

			[Com.Google.Inject.Inject]
			internal MyController(MyApp app, Controller.RequestContext ctx)
				: base(ctx)
			{
				// Note this is static so it can be in any files.
				// The app injection is optional
				this.app = app;
			}

			public override void Index()
			{
				Set("anything", "something");
			}

			public virtual void AnythingYouWant()
			{
				Set("anything", app.AnyAPI());
			}
		}

		public class MyView : HtmlPage
		{
			// Ditto
			// You can inject the app in views if needed.
			protected internal override void Render(Hamlet.HTML<HtmlPage._> html)
			{
				html.Title("My App").P("#content_id_for_css_styling").("You can have", $("anything"
					)).().();
			}
			// Note, there is no _(); (to parent element) method at root level.
			// and IDE provides instant feedback on what level you're on in
			// the auto-completion drop-downs.
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			WebApps.$for(new MyApp()).At(8888).InDevMode().Start().JoinThread();
		}
	}
}
