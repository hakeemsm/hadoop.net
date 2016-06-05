using Org.Apache.Hadoop.Record.Compiler.Generated;
using Org.Apache.Tools.Ant;
using Org.Apache.Tools.Ant.Types;


namespace Org.Apache.Hadoop.Record.Compiler.Ant
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
	public class RccTask : Task
	{
		private string language = "java";

		private FilePath src;

		private FilePath dest = new FilePath(".");

		private readonly AList<FileSet> filesets = new AList<FileSet>();

		private bool failOnError = true;

		/// <summary>Creates a new instance of RccTask</summary>
		public RccTask()
		{
		}

		/// <summary>Sets the output language option</summary>
		/// <param name="language">"java"/"c++"</param>
		public virtual void SetLanguage(string language)
		{
			this.language = language;
		}

		/// <summary>Sets the record definition file attribute</summary>
		/// <param name="file">record definition file</param>
		public virtual void SetFile(FilePath file)
		{
			this.src = file;
		}

		/// <summary>Given multiple files (via fileset), set the error handling behavior</summary>
		/// <param name="flag">true will throw build exception in case of failure (default)</param>
		public virtual void SetFailonerror(bool flag)
		{
			this.failOnError = flag;
		}

		/// <summary>Sets directory where output files will be generated</summary>
		/// <param name="dir">output directory</param>
		public virtual void SetDestdir(FilePath dir)
		{
			this.dest = dir;
		}

		/// <summary>Adds a fileset that can consist of one or more files</summary>
		/// <param name="set">Set of record definition files</param>
		public virtual void AddFileset(FileSet set)
		{
			filesets.AddItem(set);
		}

		/// <summary>Invoke the Hadoop record compiler on each record definition file</summary>
		/// <exception cref="Org.Apache.Tools.Ant.BuildException"/>
		public override void Execute()
		{
			if (src == null && filesets.Count == 0)
			{
				throw new BuildException("There must be a file attribute or a fileset child element"
					);
			}
			if (src != null)
			{
				DoCompile(src);
			}
			Project myProject = GetProject();
			for (int i = 0; i < filesets.Count; i++)
			{
				FileSet fs = filesets[i];
				DirectoryScanner ds = fs.GetDirectoryScanner(myProject);
				FilePath dir = fs.GetDir(myProject);
				string[] srcs = ds.GetIncludedFiles();
				for (int j = 0; j < srcs.Length; j++)
				{
					DoCompile(new FilePath(dir, srcs[j]));
				}
			}
		}

		/// <exception cref="Org.Apache.Tools.Ant.BuildException"/>
		private void DoCompile(FilePath file)
		{
			string[] args = new string[5];
			args[0] = "--language";
			args[1] = this.language;
			args[2] = "--destdir";
			args[3] = this.dest.GetPath();
			args[4] = file.GetPath();
			int retVal = Rcc.Driver(args);
			if (retVal != 0 && failOnError)
			{
				throw new BuildException("Hadoop record compiler returned error code " + retVal);
			}
		}
	}
}
