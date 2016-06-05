using System;
using Org.Apache.Hadoop.Yarn.Server.Webapp;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class RmView : TwoColumnLayout
	{
		internal const int MaxDisplayRows = 100;

		internal const int MaxFastRows = 1000;

		// Do NOT rename/refactor this to RMView as it will wreak havoc
		// on Mac OS HFS
		// direct table rendering
		// inline js array
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			Set(DatatablesId, "apps");
			Set(JQueryUI.InitID(Datatables, "apps"), InitAppsTable());
			SetTableStyles(html, "apps", ".queue {width:6em}", ".ui {width:8em}");
			// Set the correct title.
			string reqState = $(YarnWebParams.AppState);
			reqState = (reqState == null || reqState.IsEmpty() ? "All" : reqState);
			SetTitle(StringHelper.Sjoin(reqState, "Applications"));
		}

		protected internal virtual void CommonPreHead(Hamlet.HTML<HtmlPage._> html)
		{
			Set(AccordionId, "nav");
			Set(JQueryUI.InitID(Accordion, "nav"), "{autoHeight:false, active:0}");
		}

		protected override Type Nav()
		{
			return typeof(NavBlock);
		}

		protected override Type Content()
		{
			return typeof(AppsBlockWithMetrics);
		}

		protected internal virtual string InitAppsTable()
		{
			return WebPageUtils.AppsTableInit();
		}
	}
}
