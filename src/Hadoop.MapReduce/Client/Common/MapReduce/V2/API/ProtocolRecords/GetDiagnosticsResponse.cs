using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords
{
	public interface GetDiagnosticsResponse
	{
		IList<string> GetDiagnosticsList();

		string GetDiagnostics(int index);

		int GetDiagnosticsCount();

		void AddAllDiagnostics(IList<string> diagnostics);

		void AddDiagnostics(string diagnostic);

		void RemoveDiagnostics(int index);

		void ClearDiagnostics();
	}
}
