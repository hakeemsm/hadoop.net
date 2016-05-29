using System.Text;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webapp
{
	public class WebPageUtils
	{
		public static string AppsTableInit()
		{
			return AppsTableInit(false);
		}

		public static string AppsTableInit(bool isFairSchedulerPage)
		{
			// id, user, name, queue, starttime, finishtime, state, status, progress, ui
			// FairSchedulerPage's table is a bit different
			return JQueryUI.TableInit().Append(", 'aaData': appsTableData").Append(", bDeferRender: true"
				).Append(", bProcessing: true").Append("\n, aoColumnDefs: ").Append(GetAppsTableColumnDefs
				(isFairSchedulerPage)).Append(", aaSorting: [[0, 'desc']]}").ToString();
		}

		// Sort by id upon page load
		private static string GetAppsTableColumnDefs(bool isFairSchedulerPage)
		{
			StringBuilder sb = new StringBuilder();
			return sb.Append("[\n").Append("{'sType':'string', 'aTargets': [0]").Append(", 'mRender': parseHadoopID }"
				).Append("\n, {'sType':'numeric', 'aTargets': " + (isFairSchedulerPage ? "[6, 7]"
				 : "[5, 6]")).Append(", 'mRender': renderHadoopDate }").Append("\n, {'sType':'numeric', bSearchable:false, 'aTargets': [9]"
				).Append(", 'mRender': parseHadoopProgress }]").ToString();
		}

		public static string AttemptsTableInit()
		{
			return JQueryUI.TableInit().Append(", 'aaData': attemptsTableData").Append(", bDeferRender: true"
				).Append(", bProcessing: true").Append("\n, aoColumnDefs: ").Append(GetAttemptsTableColumnDefs
				()).Append(", aaSorting: [[0, 'desc']]}").ToString();
		}

		// Sort by id upon page load
		private static string GetAttemptsTableColumnDefs()
		{
			StringBuilder sb = new StringBuilder();
			return sb.Append("[\n").Append("{'sType':'string', 'aTargets': [0]").Append(", 'mRender': parseHadoopID }"
				).Append("\n, {'sType':'numeric', 'aTargets': [1]").Append(", 'mRender': renderHadoopDate }]"
				).ToString();
		}

		public static string ContainersTableInit()
		{
			return JQueryUI.TableInit().Append(", 'aaData': containersTableData").Append(", bDeferRender: true"
				).Append(", bProcessing: true").Append("\n, aoColumnDefs: ").Append(GetContainersTableColumnDefs
				()).Append(", aaSorting: [[0, 'desc']]}").ToString();
		}

		// Sort by id upon page load
		private static string GetContainersTableColumnDefs()
		{
			StringBuilder sb = new StringBuilder();
			return sb.Append("[\n").Append("{'sType':'string', 'aTargets': [0]").Append(", 'mRender': parseHadoopID }]"
				).ToString();
		}
	}
}
