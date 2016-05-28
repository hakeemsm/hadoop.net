using System.IO;
using Com.Google.Common.Base;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>
	/// TextWriterImageProcessor mixes in the ability for ImageVisitor
	/// implementations to easily write their output to a text file.
	/// </summary>
	/// <remarks>
	/// TextWriterImageProcessor mixes in the ability for ImageVisitor
	/// implementations to easily write their output to a text file.
	/// Implementing classes should be sure to call the super methods for the
	/// constructors, finish and finishAbnormally methods, in order that the
	/// underlying file may be opened and closed correctly.
	/// Note, this class does not add newlines to text written to file or (if
	/// enabled) screen.  This is the implementing class' responsibility.
	/// </remarks>
	internal abstract class TextWriterImageVisitor : ImageVisitor
	{
		private bool printToScreen = false;

		private bool okToWrite = false;

		private readonly OutputStreamWriter fw;

		/// <summary>Create a processor that writes to the file named.</summary>
		/// <param name="filename">Name of file to write output to</param>
		/// <exception cref="System.IO.IOException"/>
		public TextWriterImageVisitor(string filename)
			: this(filename, false)
		{
		}

		/// <summary>
		/// Create a processor that writes to the file named and may or may not
		/// also output to the screen, as specified.
		/// </summary>
		/// <param name="filename">Name of file to write output to</param>
		/// <param name="printToScreen">Mirror output to screen?</param>
		/// <exception cref="System.IO.IOException"/>
		public TextWriterImageVisitor(string filename, bool printToScreen)
			: base()
		{
			this.printToScreen = printToScreen;
			fw = new OutputStreamWriter(new FileOutputStream(filename), Charsets.Utf8);
			okToWrite = true;
		}

		/* (non-Javadoc)
		* @see org.apache.hadoop.hdfs.tools.offlineImageViewer.ImageVisitor#finish()
		*/
		/// <exception cref="System.IO.IOException"/>
		internal override void Finish()
		{
			Close();
		}

		/* (non-Javadoc)
		* @see org.apache.hadoop.hdfs.tools.offlineImageViewer.ImageVisitor#finishAbnormally()
		*/
		/// <exception cref="System.IO.IOException"/>
		internal override void FinishAbnormally()
		{
			Close();
		}

		/// <summary>Close output stream and prevent further writing</summary>
		/// <exception cref="System.IO.IOException"/>
		private void Close()
		{
			fw.Close();
			okToWrite = false;
		}

		/// <summary>Write parameter to output file (and possibly screen).</summary>
		/// <param name="toWrite">Text to write to file</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void Write(string toWrite)
		{
			if (!okToWrite)
			{
				throw new IOException("file not open for writing.");
			}
			if (printToScreen)
			{
				System.Console.Out.Write(toWrite);
			}
			try
			{
				fw.Write(toWrite);
			}
			catch (IOException e)
			{
				okToWrite = false;
				throw;
			}
		}
	}
}
