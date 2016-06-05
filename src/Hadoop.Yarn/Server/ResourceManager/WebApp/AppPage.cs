using System;
using Org.Apache.Hadoop.Yarn.Server.Webapp;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class AppPage : RmView
	{
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			string appId = $(YarnWebParams.ApplicationId);
			Set(Title, appId.IsEmpty() ? "Bad request: missing application ID" : StringHelper.Join
				("Application ", $(YarnWebParams.ApplicationId)));
			Set(JQueryUI.DatatablesId, "attempts ResourceRequests");
			Set(JQueryUI.InitID(JQueryUI.Datatables, "attempts"), WebPageUtils.AttemptsTableInit
				());
			SetTableStyles(html, "attempts", ".queue {width:6em}", ".ui {width:8em}");
			SetTableStyles(html, "ResourceRequests");
			Set(YarnWebParams.WebUiType, YarnWebParams.RmWebUi);
		}

		protected override Type Content()
		{
			return typeof(RMAppBlock);
		}
	}
}
