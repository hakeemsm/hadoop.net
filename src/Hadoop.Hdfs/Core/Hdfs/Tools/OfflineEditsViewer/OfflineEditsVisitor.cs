using System;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineEditsViewer
{
	/// <summary>
	/// An implementation of OfflineEditsVisitor can traverse the structure of an
	/// Hadoop edits log and respond to each of the structures within the file.
	/// </summary>
	public interface OfflineEditsVisitor
	{
		/// <summary>Begin visiting the edits log structure.</summary>
		/// <remarks>
		/// Begin visiting the edits log structure.  Opportunity to perform
		/// any initialization necessary for the implementing visitor.
		/// </remarks>
		/// <param name="version">Edit log version</param>
		/// <exception cref="System.IO.IOException"/>
		void Start(int version);

		/// <summary>Finish visiting the edits log structure.</summary>
		/// <remarks>
		/// Finish visiting the edits log structure.  Opportunity to perform any
		/// clean up necessary for the implementing visitor.
		/// </remarks>
		/// <param name="error">
		/// If the visitor was closed because of an
		/// unrecoverable error in the input stream, this
		/// is the exception.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		void Close(Exception error);

		/// <summary>
		/// Begin visiting an element that encloses another element, such as
		/// the beginning of the list of blocks that comprise a file.
		/// </summary>
		/// <param name="value">Token being visited</param>
		/// <exception cref="System.IO.IOException"/>
		void VisitOp(FSEditLogOp op);
	}
}
