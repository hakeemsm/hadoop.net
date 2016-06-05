using System.Collections.Generic;


namespace Org.Apache.Hadoop.Record.Compiler
{
	/// <summary>Java Code generator front-end for Hadoop record I/O.</summary>
	internal class JavaGenerator : CodeGenerator
	{
		internal JavaGenerator()
		{
		}

		/// <summary>Generate Java code for records.</summary>
		/// <remarks>
		/// Generate Java code for records. This method is only a front-end to
		/// JRecord, since one file is generated for each record.
		/// </remarks>
		/// <param name="name">possibly full pathname to the file</param>
		/// <param name="ilist">included files (as JFile)</param>
		/// <param name="rlist">List of records defined within this file</param>
		/// <param name="destDir">output directory</param>
		/// <exception cref="System.IO.IOException"/>
		internal override void GenCode(string name, AList<JFile> ilist, AList<JRecord> rlist
			, string destDir, AList<string> options)
		{
			for (IEnumerator<JRecord> iter = rlist.GetEnumerator(); iter.HasNext(); )
			{
				JRecord rec = iter.Next();
				rec.GenJavaCode(destDir, options);
			}
		}
	}
}
