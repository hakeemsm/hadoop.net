using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	public class JQueryUI : HtmlBlock
	{
		public const string Accordion = "ui.accordion";

		public const string AccordionId = Accordion + ".id";

		public const string Datatables = "ui.dataTables";

		public const string DatatablesId = Datatables + ".id";

		public const string DatatablesSelector = Datatables + ".selector";

		public const string Dialog = "ui.dialog";

		public const string DialogId = Dialog + ".id";

		public const string DialogSelector = Dialog + ".selector";

		public const string Progressbar = "ui.progressbar";

		public const string ProgressbarId = Progressbar + ".id";

		public const string Progressbar = ".ui-progressbar.ui-widget.ui-widget-content.ui-corner-all";

		public static readonly string CProgressbar = Progressbar.Replace('.', ' ').Trim();

		public const string ProgressbarValue = ".ui-progressbar-value.ui-widget-header.ui-corner-left";

		public static readonly string CProgressbarValue = ProgressbarValue.Replace('.', ' '
			).Trim();

		public const string InfoWrap = ".info-wrap.ui-widget-content.ui-corner-bottom";

		public const string Th = ".ui-state-default";

		public static readonly string CTh = Th.Replace('.', ' ').Trim();

		public const string CTable = "table";

		public const string Info = ".info";

		public const string Odd = ".odd";

		public const string Even = ".even";

		// UI params
		// common CSS classes
		protected internal override void Render(HtmlBlock.Block html)
		{
			html.Link(Root_url("static/jquery/themes-1.9.1/base/jquery-ui.css")).Link(Root_url
				("static/dt-1.9.4/css/jui-dt.css")).Script(Root_url("static/jquery/jquery-1.8.2.min.js"
				)).Script(Root_url("static/jquery/jquery-ui-1.9.1.custom.min.js")).Script(Root_url
				("static/dt-1.9.4/js/jquery.dataTables.min.js")).Script(Root_url("static/yarn.dt.plugins.js"
				)).Style("#jsnotice { padding: 0.2em; text-align: center; }", ".ui-progressbar { height: 1em; min-width: 5em }"
				);
			// required
			IList<string> list = Lists.NewArrayList();
			InitAccordions(list);
			InitDataTables(list);
			InitDialogs(list);
			InitProgressBars(list);
			if (!list.IsEmpty())
			{
				html.Script().$type("text/javascript").("$(function() {").(Sharpen.Collections.ToArray
					(list)).("});").();
			}
		}

		public static void Jsnotice(HamletSpec.HTML html)
		{
			html.Div("#jsnotice.ui-state-error").("This page will not function without javascript enabled."
				 + " Please enable javascript on your browser.").();
			html.Script().$type("text/javascript").("$('#jsnotice').hide();").();
		}

		protected internal virtual void InitAccordions(IList<string> list)
		{
			foreach (string id in StringHelper.Split($(AccordionId)))
			{
				if (Html.IsValidId(id))
				{
					string init = $(InitID(Accordion, id));
					if (init.IsEmpty())
					{
						init = "{autoHeight: false}";
					}
					list.AddItem(StringHelper.Join("  $('#", id, "').accordion(", init, ");"));
				}
			}
		}

		protected internal virtual void InitDataTables(IList<string> list)
		{
			string defaultInit = "{bJQueryUI: true, sPaginationType: 'full_numbers'}";
			string stateSaveInit = "bStateSave : true, " + "\"fnStateSave\": function (oSettings, oData) { "
				 + " data = oData.aoSearchCols;" + "for(i =0 ; i < data.length; i ++) {" + "data[i].sSearch = \"\""
				 + "}" + " sessionStorage.setItem( oSettings.sTableId, JSON.stringify(oData) ); }, "
				 + "\"fnStateLoad\": function (oSettings) { " + "return JSON.parse( sessionStorage.getItem(oSettings.sTableId) );}, ";
			foreach (string id in StringHelper.Split($(DatatablesId)))
			{
				if (Html.IsValidId(id))
				{
					string init = $(InitID(Datatables, id));
					if (init.IsEmpty())
					{
						init = defaultInit;
					}
					// for inserting stateSaveInit
					int pos = init.IndexOf('{') + 1;
					init = new StringBuilder(init).Insert(pos, stateSaveInit).ToString();
					list.AddItem(StringHelper.Join(id, "DataTable =  $('#", id, "').dataTable(", init
						, ").fnSetFilteringDelay(188);"));
					string postInit = $(PostInitID(Datatables, id));
					if (!postInit.IsEmpty())
					{
						list.AddItem(postInit);
					}
				}
			}
			string selector = $(DatatablesSelector);
			if (!selector.IsEmpty())
			{
				string init = $(InitSelector(Datatables));
				if (init.IsEmpty())
				{
					init = defaultInit;
				}
				int pos = init.IndexOf('{') + 1;
				init = new StringBuilder(init).Insert(pos, stateSaveInit).ToString();
				list.AddItem(StringHelper.Join("  $('", StringEscapeUtils.EscapeJavaScript(selector
					), "').dataTable(", init, ").fnSetFilteringDelay(288);"));
			}
		}

		protected internal virtual void InitDialogs(IList<string> list)
		{
			string defaultInit = "{autoOpen: false, show: transfer, hide: explode}";
			foreach (string id in StringHelper.Split($(DialogId)))
			{
				if (Html.IsValidId(id))
				{
					string init = $(InitID(Dialog, id));
					if (init.IsEmpty())
					{
						init = defaultInit;
					}
					string opener = $(StringHelper.Djoin(Dialog, id, "opener"));
					list.AddItem(StringHelper.Join("  $('#", id, "').dialog(", init, ");"));
					if (!opener.IsEmpty() && Html.IsValidId(opener))
					{
						list.AddItem(StringHelper.Join("  $('#", opener, "').click(function() { ", "$('#"
							, id, "').dialog('open'); return false; });"));
					}
				}
			}
			string selector = $(DialogSelector);
			if (!selector.IsEmpty())
			{
				string init = $(InitSelector(Dialog));
				if (init.IsEmpty())
				{
					init = defaultInit;
				}
				list.AddItem(StringHelper.Join("  $('", StringEscapeUtils.EscapeJavaScript(selector
					), "').click(function() { $(this).children('.dialog').dialog(", init, "); return false; });"
					));
			}
		}

		protected internal virtual void InitProgressBars(IList<string> list)
		{
			foreach (string id in StringHelper.Split($(ProgressbarId)))
			{
				if (Html.IsValidId(id))
				{
					string init = $(InitID(Progressbar, id));
					list.AddItem(StringHelper.Join("  $('#", id, "').progressbar(", init, ");"));
				}
			}
		}

		public static string InitID(string name, string id)
		{
			return StringHelper.Djoin(name, id, "init");
		}

		public static string PostInitID(string name, string id)
		{
			return StringHelper.Djoin(name, id, "postinit");
		}

		public static string InitSelector(string name)
		{
			return StringHelper.Djoin(name, "selector.init");
		}

		public static StringBuilder TableInit()
		{
			return new StringBuilder("{bJQueryUI:true, ").Append("sPaginationType: 'full_numbers', iDisplayLength:20, "
				).Append("aLengthMenu:[20, 40, 60, 80, 100]");
		}
	}
}
