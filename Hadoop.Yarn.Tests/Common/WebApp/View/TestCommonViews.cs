using Com.Google.Inject;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	public class TestCommonViews
	{
		[NUnit.Framework.Test]
		public virtual void TestErrorPage()
		{
			Injector injector = WebAppTests.TestPage(typeof(ErrorPage));
		}

		[NUnit.Framework.Test]
		public virtual void TestHeaderBlock()
		{
			WebAppTests.TestBlock(typeof(HeaderBlock));
		}

		[NUnit.Framework.Test]
		public virtual void TestFooterBlock()
		{
			WebAppTests.TestBlock(typeof(FooterBlock));
		}

		[NUnit.Framework.Test]
		public virtual void TestJQueryUI()
		{
			WebAppTests.TestBlock(typeof(JQueryUI));
		}

		[NUnit.Framework.Test]
		public virtual void TestInfoBlock()
		{
			Injector injector = WebAppTests.CreateMockInjector(this);
			ResponseInfo info = injector.GetInstance<ResponseInfo>();
		}
	}
}
