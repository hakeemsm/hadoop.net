using System;
using Org.Apache.Hadoop.Yarn.Server.Webapp;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class ContainerPage : RmView
	{
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			string containerId = $(YarnWebParams.ContainerId);
			Set(Title, containerId.IsEmpty() ? "Bad request: missing container ID" : StringHelper.Join
				("Container ", $(YarnWebParams.ContainerId)));
		}

		protected override Type Content()
		{
			return typeof(ContainerBlock);
		}
	}
}
