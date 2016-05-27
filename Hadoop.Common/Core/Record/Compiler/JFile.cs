using Sharpen;

namespace Org.Apache.Hadoop.Record.Compiler
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
		private AList<Org.Apache.Hadoop.Record.Compiler.JFile> mInclFiles;

		/// <summary>Ordered list of records declared in this file</summary>
		private AList<JRecord> mRecords;

		/// <summary>Creates a new instance of JFile</summary>
		/// <param name="name">possibly full pathname to the file</param>
		/// <param name="inclFiles">included files (as JFile)</param>
		/// <param name="recList">List of records defined within this file</param>
		public JFile(string name, AList<Org.Apache.Hadoop.Record.Compiler.JFile> inclFiles
			, AList<JRecord> recList)
		{
			mName = name;
			mInclFiles = inclFiles;
			mRecords = recList;
		}

		/// <summary>Strip the other pathname components and return the basename</summary>
		internal virtual string GetName()
		{
			int idx = mName.LastIndexOf('/');
			return (idx > 0) ? Sharpen.Runtime.Substring(mName, idx) : mName;
		}

		/// <summary>Generate record code in given language.</summary>
		/// <remarks>
		/// Generate record code in given language. Language should be all
		/// lowercase.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual int GenCode(string language, string destDir, AList<string> options
			)
		{
			CodeGenerator gen = CodeGenerator.Get(language);
			if (gen != null)
			{
				gen.GenCode(mName, mInclFiles, mRecords, destDir, options);
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
