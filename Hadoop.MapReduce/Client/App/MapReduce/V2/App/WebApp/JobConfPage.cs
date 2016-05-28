using System;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	/// <summary>Render a page with the configuration for a given job in it.</summary>
	public class JobConfPage : AppView
	{
		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
		*/
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			string jobID = $(AMParams.JobId);
			Set(Title, jobID.IsEmpty() ? "Bad request: missing job ID" : StringHelper.Join("Configuration for MapReduce Job "
				, $(AMParams.JobId)));
			CommonPreHead(html);
			Set(JQueryUI.InitID(JQueryUI.Accordion, "nav"), "{autoHeight:false, active:2}");
			Set(JQueryUI.DatatablesId, "conf");
			Set(JQueryUI.InitID(JQueryUI.Datatables, "conf"), ConfTableInit());
			Set(JQueryUI.PostInitID(JQueryUI.Datatables, "conf"), ConfPostTableInit());
			SetTableStyles(html, "conf");
		}

		/// <summary>The body of this block is the configuration block.</summary>
		/// <returns>ConfBlock.class</returns>
		protected override Type Content()
		{
			return typeof(ConfBlock);
		}

		/// <returns>
		/// the end of the JS map that is the jquery datatable config for the
		/// conf table.
		/// </returns>
		private string ConfTableInit()
		{
			return JQueryUI.TableInit().Append("}").ToString();
		}

		/// <returns>
		/// the java script code to allow the jquery conf datatable to filter
		/// by column.
		/// </returns>
		private string ConfPostTableInit()
		{
			return "var confInitVals = new Array();\n" + "$('tfoot input').keyup( function () \n{"
				 + "  confDataTable.fnFilter( this.value, $('tfoot input').index(this) );\n" + "} );\n"
				 + "$('tfoot input').each( function (i) {\n" + "  confInitVals[i] = this.value;\n"
				 + "} );\n" + "$('tfoot input').focus( function () {\n" + "  if ( this.className == 'search_init' )\n"
				 + "  {\n" + "    this.className = '';\n" + "    this.value = '';\n" + "  }\n" +
				 "} );\n" + "$('tfoot input').blur( function (i) {\n" + "  if ( this.value == '' )\n"
				 + "  {\n" + "    this.className = 'search_init';\n" + "    this.value = confInitVals[$('tfoot input').index(this)];\n"
				 + "  }\n" + "} );\n";
		}
	}
}
