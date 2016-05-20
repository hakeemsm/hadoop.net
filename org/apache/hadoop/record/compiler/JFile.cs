using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	/// <summary>Container for the Hadoop Record DDL.</summary>
	/// <remarks>
	/// Container for the Hadoop Record DDL.
	/// The main components of the file are filename, list of included files,
	/// and records defined in that file.
	/// </remarks>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JFile
	{
		/// <summary>Possibly full name of the file</summary>
		private string mName;

		/// <summary>Ordered list of included files</summary>
		private System.Collections.Generic.List<org.apache.hadoop.record.compiler.JFile> 
			mInclFiles;

		/// <summary>Ordered list of records declared in this file</summary>
		private System.Collections.Generic.List<org.apache.hadoop.record.compiler.JRecord
			> mRecords;

		/// <summary>Creates a new instance of JFile</summary>
		/// <param name="name">possibly full pathname to the file</param>
		/// <param name="inclFiles">included files (as JFile)</param>
		/// <param name="recList">List of records defined within this file</param>
		public JFile(string name, System.Collections.Generic.List<org.apache.hadoop.record.compiler.JFile
			> inclFiles, System.Collections.Generic.List<org.apache.hadoop.record.compiler.JRecord
			> recList)
		{
			mName = name;
			mInclFiles = inclFiles;
			mRecords = recList;
		}

		/// <summary>Strip the other pathname components and return the basename</summary>
		internal virtual string getName()
		{
			int idx = mName.LastIndexOf('/');
			return (idx > 0) ? Sharpen.Runtime.substring(mName, idx) : mName;
		}

		/// <summary>Generate record code in given language.</summary>
		/// <remarks>
		/// Generate record code in given language. Language should be all
		/// lowercase.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual int genCode(string language, string destDir, System.Collections.Generic.List
			<string> options)
		{
			org.apache.hadoop.record.compiler.CodeGenerator gen = org.apache.hadoop.record.compiler.CodeGenerator
				.get(language);
			if (gen != null)
			{
				gen.genCode(mName, mInclFiles, mRecords, destDir, options);
			}
			else
			{
				System.Console.Error.WriteLine("Cannnot recognize language:" + language);
				return 1;
			}
			return 0;
		}
	}
}
