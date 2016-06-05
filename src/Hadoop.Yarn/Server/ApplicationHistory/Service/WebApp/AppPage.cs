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
	public class AppPage : AHSView
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
			Set(YarnWebParams.WebUiType, YarnWebParams.AppHistoryWebUi);
		}

		protected override Type Content()
		{
			return typeof(AppBlock);
		}

		protected internal virtual string GetAttemptsTableColumnDefs()
		{
			StringBuilder sb = new StringBuilder();
			return sb.Append("[\n").Append("{'sType':'string', 'aTargets': [0]").Append(", 'mRender': parseHadoopID }"
				).Append("\n, {'sType':'numeric', 'aTargets': [1]").Append(", 'mRender': renderHadoopDate }]"
				).ToString();
		}
	}
}
