using System;
using Org.Apache.Hadoop.Yarn.Server.Webapp;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Webapp
{
	public class AHSView : TwoColumnLayout
	{
		internal const int MaxDisplayRows = 100;

		internal const int MaxFastRows = 1000;

		// Do NOT rename/refactor this to AHSView as it will wreak havoc
		// on Mac OS HFS
		// direct table rendering
		// inline js array
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			Set(JQueryUI.DatatablesId, "apps");
			Set(JQueryUI.InitID(JQueryUI.Datatables, "apps"), WebPageUtils.AppsTableInit());
			SetTableStyles(html, "apps", ".queue {width:6em}", ".ui {width:8em}");
			// Set the correct title.
			string reqState = $(YarnWebParams.AppState);
			reqState = (reqState == null || reqState.IsEmpty() ? "All" : reqState);
			SetTitle(StringHelper.Sjoin(reqState, "Applications"));
		}

		protected internal virtual void CommonPreHead(Hamlet.HTML<HtmlPage._> html)
		{
			Set(JQueryUI.AccordionId, "nav");
			Set(JQueryUI.InitID(JQueryUI.Accordion, "nav"), "{autoHeight:false, active:0}");
		}

		protected override Type Nav()
		{
			return typeof(NavBlock);
		}

		protected override Type Content()
		{
			return typeof(AppsBlock);
		}
	}
}
