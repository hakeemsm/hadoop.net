using System;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.View;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	/// <summary>A view that should be used as the base class for all history server pages.
	/// 	</summary>
	public class HsView : TwoColumnLayout
	{
		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.yarn.webapp.view.TwoColumnLayout#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
		*/
		protected override void PreHead(Hamlet.HTML<HtmlPage._> html)
		{
			CommonPreHead(html);
			Set(JQueryUI.DatatablesId, "jobs");
			Set(JQueryUI.InitID(JQueryUI.Datatables, "jobs"), JobsTableInit());
			Set(JQueryUI.PostInitID(JQueryUI.Datatables, "jobs"), JobsPostTableInit());
			SetTableStyles(html, "jobs");
		}

		/// <summary>The prehead that should be common to all subclasses.</summary>
		/// <param name="html">used to render.</param>
		protected internal virtual void CommonPreHead(Hamlet.HTML<HtmlPage._> html)
		{
			Set(JQueryUI.AccordionId, "nav");
			Set(JQueryUI.InitID(JQueryUI.Accordion, "nav"), "{autoHeight:false, active:0}");
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.yarn.webapp.view.TwoColumnLayout#nav()
		*/
		protected override Type Nav()
		{
			return typeof(HsNavBlock);
		}

		/*
		* (non-Javadoc)
		* @see org.apache.hadoop.yarn.webapp.view.TwoColumnLayout#content()
		*/
		protected override Type Content()
		{
			return typeof(HsJobsBlock);
		}

		//TODO We need a way to move all of the javascript/CSS that is for a subview
		// into that subview.
		/// <returns>
		/// The end of a javascript map that is the jquery datatable
		/// configuration for the jobs table.  the Jobs table is assumed to be
		/// rendered by the class returned from
		/// <see cref="Content()"/>
		/// 
		/// </returns>
		private string JobsTableInit()
		{
			return JQueryUI.TableInit().Append(", 'aaData': jobsTableData").Append(", bDeferRender: true"
				).Append(", bProcessing: true").Append(", aaSorting: [[3, 'desc']]").Append(", aoColumnDefs:["
				).Append("{'sType':'numeric', 'bSearchable': false" + ", 'aTargets': [ 8, 9, 10, 11 ] }"
				).Append("]}").ToString();
		}

		// Sort by id upon page load
		// Maps Total, Maps Completed, Reduces Total and Reduces Completed
		/// <returns>
		/// javascript to add into the jquery block after the table has
		/// been initialized. This code adds in per field filtering.
		/// </returns>
		private string JobsPostTableInit()
		{
			return "var asInitVals = new Array();\n" + "$('tfoot input').keyup( function () \n{"
				 + "  jobsDataTable.fnFilter( this.value, $('tfoot input').index(this) );\n" + "} );\n"
				 + "$('tfoot input').each( function (i) {\n" + "  asInitVals[i] = this.value;\n"
				 + "} );\n" + "$('tfoot input').focus( function () {\n" + "  if ( this.className == 'search_init' )\n"
				 + "  {\n" + "    this.className = '';\n" + "    this.value = '';\n" + "  }\n" +
				 "} );\n" + "$('tfoot input').blur( function (i) {\n" + "  if ( this.value == '' )\n"
				 + "  {\n" + "    this.className = 'search_init';\n" + "    this.value = asInitVals[$('tfoot input').index(this)];\n"
				 + "  }\n" + "} );\n";
		}
	}
}
