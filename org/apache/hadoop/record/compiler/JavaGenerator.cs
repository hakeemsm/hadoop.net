using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	/// <summary>Java Code generator front-end for Hadoop record I/O.</summary>
	internal class JavaGenerator : org.apache.hadoop.record.compiler.CodeGenerator
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
		internal override void genCode(string name, System.Collections.Generic.List<org.apache.hadoop.record.compiler.JFile
			> ilist, System.Collections.Generic.List<org.apache.hadoop.record.compiler.JRecord
			> rlist, string destDir, System.Collections.Generic.List<string> options)
		{
			for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JRecord
				> iter = rlist.GetEnumerator(); iter.MoveNext(); )
			{
				org.apache.hadoop.record.compiler.JRecord rec = iter.Current;
				rec.genJavaCode(destDir, options);
			}
		}
	}
}
