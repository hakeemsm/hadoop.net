using Sharpen;

namespace org.apache.hadoop.record.compiler.ant
{
	/// <summary>
	/// Hadoop record compiler ant Task
	/// <p> This task takes the given record definition files and compiles them into
	/// java or c++
	/// files.
	/// </summary>
	/// <remarks>
	/// Hadoop record compiler ant Task
	/// <p> This task takes the given record definition files and compiles them into
	/// java or c++
	/// files. It is then up to the user to compile the generated files.
	/// <p> The task requires the <code>file</code> or the nested fileset element to be
	/// specified. Optional attributes are <code>language</code> (set the output
	/// language, default is "java"),
	/// <code>destdir</code> (name of the destination directory for generated java/c++
	/// code, default is ".") and <code>failonerror</code> (specifies error handling
	/// behavior. default is true).
	/// <p><h4>Usage</h4>
	/// <pre>
	/// &lt;recordcc
	/// destdir="${basedir}/gensrc"
	/// language="java"&gt;
	/// &lt;fileset include="**\/*.jr" /&gt;
	/// &lt;/recordcc&gt;
	/// </pre>
	/// </remarks>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class RccTask : org.apache.tools.ant.Task
	{
		private string language = "java";

		private java.io.File src;

		private java.io.File dest = new java.io.File(".");

		private readonly System.Collections.Generic.List<org.apache.tools.ant.types.FileSet
			> filesets = new System.Collections.Generic.List<org.apache.tools.ant.types.FileSet
			>();

		private bool failOnError = true;

		/// <summary>Creates a new instance of RccTask</summary>
		public RccTask()
		{
		}

		/// <summary>Sets the output language option</summary>
		/// <param name="language">"java"/"c++"</param>
		public virtual void setLanguage(string language)
		{
			this.language = language;
		}

		/// <summary>Sets the record definition file attribute</summary>
		/// <param name="file">record definition file</param>
		public virtual void setFile(java.io.File file)
		{
			this.src = file;
		}

		/// <summary>Given multiple files (via fileset), set the error handling behavior</summary>
		/// <param name="flag">true will throw build exception in case of failure (default)</param>
		public virtual void setFailonerror(bool flag)
		{
			this.failOnError = flag;
		}

		/// <summary>Sets directory where output files will be generated</summary>
		/// <param name="dir">output directory</param>
		public virtual void setDestdir(java.io.File dir)
		{
			this.dest = dir;
		}

		/// <summary>Adds a fileset that can consist of one or more files</summary>
		/// <param name="set">Set of record definition files</param>
		public virtual void addFileset(org.apache.tools.ant.types.FileSet set)
		{
			filesets.add(set);
		}

		/// <summary>Invoke the Hadoop record compiler on each record definition file</summary>
		/// <exception cref="org.apache.tools.ant.BuildException"/>
		public override void execute()
		{
			if (src == null && filesets.Count == 0)
			{
				throw new org.apache.tools.ant.BuildException("There must be a file attribute or a fileset child element"
					);
			}
			if (src != null)
			{
				doCompile(src);
			}
			org.apache.tools.ant.Project myProject = getProject();
			for (int i = 0; i < filesets.Count; i++)
			{
				org.apache.tools.ant.types.FileSet fs = filesets[i];
				org.apache.tools.ant.DirectoryScanner ds = fs.getDirectoryScanner(myProject);
				java.io.File dir = fs.getDir(myProject);
				string[] srcs = ds.getIncludedFiles();
				for (int j = 0; j < srcs.Length; j++)
				{
					doCompile(new java.io.File(dir, srcs[j]));
				}
			}
		}

		/// <exception cref="org.apache.tools.ant.BuildException"/>
		private void doCompile(java.io.File file)
		{
			string[] args = new string[5];
			args[0] = "--language";
			args[1] = this.language;
			args[2] = "--destdir";
			args[3] = this.dest.getPath();
			args[4] = file.getPath();
			int retVal = org.apache.hadoop.record.compiler.generated.Rcc.driver(args);
			if (retVal != 0 && failOnError)
			{
				throw new org.apache.tools.ant.BuildException("Hadoop record compiler returned error code "
					 + retVal);
			}
		}
	}
}
