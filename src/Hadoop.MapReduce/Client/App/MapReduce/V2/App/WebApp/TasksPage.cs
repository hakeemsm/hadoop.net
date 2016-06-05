using System;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	public class TasksPage : AppView
	{
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			Set(DatatablesId, "tasks");
			Set(JQueryUI.InitID(Accordion, "nav"), "{autoHeight:false, active:2}");
			Set(JQueryUI.InitID(Datatables, "tasks"), TasksTableInit());
			SetTableStyles(html, "tasks");
		}

		protected override Type Content()
		{
			return typeof(TasksBlock);
		}

		private string TasksTableInit()
		{
			return JQueryUI.TableInit().Append(", 'aaData': tasksTableData").Append(", bDeferRender: true"
				).Append(", bProcessing: true").Append("\n, aoColumnDefs: [\n").Append("{'sType':'string', 'aTargets': [0]"
				).Append(", 'mRender': parseHadoopID }").Append("\n, {'sType':'numeric', bSearchable:false, 'aTargets': [1]"
				).Append(", 'mRender': parseHadoopProgress }").Append("\n, {'sType':'numeric', 'aTargets': [4, 5]"
				).Append(", 'mRender': renderHadoopDate }").Append("\n, {'sType':'numeric', 'aTargets': [6]"
				).Append(", 'mRender': renderHadoopElapsedTime }]").Append(", aaSorting: [[0, 'asc']] }"
				).ToString();
		}
		// Sort by id upon page load
	}
}
