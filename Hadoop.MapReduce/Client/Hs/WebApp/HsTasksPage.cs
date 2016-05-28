using System;
using System.Text;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	/// <summary>A page showing the tasks for a given application.</summary>
	public class HsTasksPage : HsView
	{
		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
		*/
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			Set(JQueryUI.DatatablesId, "tasks");
			Set(JQueryUI.DatatablesSelector, ".dt-tasks");
			Set(JQueryUI.InitSelector(JQueryUI.Datatables), TasksTableInit());
			Set(JQueryUI.InitID(JQueryUI.Accordion, "nav"), "{autoHeight:false, active:1}");
			Set(JQueryUI.InitID(JQueryUI.Datatables, "tasks"), TasksTableInit());
			Set(JQueryUI.PostInitID(JQueryUI.Datatables, "tasks"), JobsPostTableInit());
			SetTableStyles(html, "tasks");
		}

		/// <summary>The content of this page is the TasksBlock</summary>
		/// <returns>HsTasksBlock.class</returns>
		protected override Type Content()
		{
			return typeof(HsTasksBlock);
		}

		/// <returns>
		/// the end of the JS map that is the jquery datatable configuration
		/// for the tasks table.
		/// </returns>
		private string TasksTableInit()
		{
			TaskType type = null;
			string symbol = $(AMParams.TaskType);
			if (!symbol.IsEmpty())
			{
				type = MRApps.TaskType(symbol);
			}
			StringBuilder b = JQueryUI.TableInit().Append(", 'aaData': tasksTableData").Append
				(", bDeferRender: true").Append(", bProcessing: true").Append("\n, aoColumnDefs: [\n"
				).Append("{'sType':'string', 'aTargets': [ 0 ]").Append(", 'mRender': parseHadoopID }"
				).Append(", {'sType':'numeric', 'aTargets': [ 4").Append(type == TaskType.Reduce
				 ? ", 9, 10, 11, 12" : ", 7").Append(" ], 'mRender': renderHadoopElapsedTime }")
				.Append("\n, {'sType':'numeric', 'aTargets': [ 2, 3, 5").Append(type == TaskType
				.Reduce ? ", 6, 7, 8" : ", 6").Append(" ], 'mRender': renderHadoopDate }]").Append
				("\n, aaSorting: [[0, 'asc']]").Append("}");
			// Sort by id upon page load
			return b.ToString();
		}

		private string JobsPostTableInit()
		{
			return "var asInitVals = new Array();\n" + "$('tfoot input').keyup( function () \n{"
				 + "  tasksDataTable.fnFilter( this.value, $('tfoot input').index(this) );\n" + 
				"} );\n" + "$('tfoot input').each( function (i) {\n" + "  asInitVals[i] = this.value;\n"
				 + "} );\n" + "$('tfoot input').focus( function () {\n" + "  if ( this.className == 'search_init' )\n"
				 + "  {\n" + "    this.className = '';\n" + "    this.value = '';\n" + "  }\n" +
				 "} );\n" + "$('tfoot input').blur( function (i) {\n" + "  if ( this.value == '' )\n"
				 + "  {\n" + "    this.className = 'search_init';\n" + "    this.value = asInitVals[$('tfoot input').index(this)];\n"
				 + "  }\n" + "} );\n";
		}
	}
}
