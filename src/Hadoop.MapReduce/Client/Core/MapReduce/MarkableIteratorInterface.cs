using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// <code>MarkableIteratorInterface</code> is an interface for a iterator that
	/// supports mark-reset functionality.
	/// </summary>
	/// <remarks>
	/// <code>MarkableIteratorInterface</code> is an interface for a iterator that
	/// supports mark-reset functionality.
	/// <p>Mark can be called at any point during the iteration process and a reset
	/// will go back to the last record before the call to the previous mark.
	/// </remarks>
	internal interface MarkableIteratorInterface<Value> : IEnumerator<VALUE>
	{
		/// <summary>Mark the current record.</summary>
		/// <remarks>
		/// Mark the current record. A subsequent call to reset will rewind
		/// the iterator to this record.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		void Mark();

		/// <summary>Reset the iterator to the last record before a call to the previous mark
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		void Reset();

		/// <summary>Clear any previously set mark</summary>
		/// <exception cref="System.IO.IOException"/>
		void ClearMark();
	}
}
