using System;
using Org.Apache.Hadoop.Yarn.Server.Webapp;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class AppAttemptPage : RmView
	{
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			string appAttemptId = $(YarnWebParams.ApplicationAttemptId);
			Set(Title, appAttemptId.IsEmpty() ? "Bad request: missing application attempt ID"
				 : StringHelper.Join("Application Attempt ", $(YarnWebParams.ApplicationAttemptId
				)));
			Set(JQueryUI.DatatablesId, "containers");
			Set(JQueryUI.InitID(JQueryUI.Datatables, "containers"), WebPageUtils.ContainersTableInit
				());
			SetTableStyles(html, "containers", ".queue {width:6em}", ".ui {width:8em}");
			Set(YarnWebParams.WebUiType, YarnWebParams.RmWebUi);
		}

		protected override Type Content()
		{
			return typeof(RMAppAttemptBlock);
		}
	}
}
