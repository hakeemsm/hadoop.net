using System;
using System.Text;
using Org.Apache.Hadoop.Yarn.Server.Webapp;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Webapp
{
	public class AppAttemptPage : AHSView
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
			Set(YarnWebParams.WebUiType, YarnWebParams.AppHistoryWebUi);
		}

		protected override Type Content()
		{
			return typeof(AppAttemptBlock);
		}

		protected internal virtual string GetContainersTableColumnDefs()
		{
			StringBuilder sb = new StringBuilder();
			return sb.Append("[\n").Append("{'sType':'string', 'aTargets': [0]").Append(", 'mRender': parseHadoopID }]"
				).ToString();
		}
	}
}
