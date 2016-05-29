using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public interface RegisterNodeManagerResponse
	{
		MasterKey GetContainerTokenMasterKey();

		void SetContainerTokenMasterKey(MasterKey secretKey);

		MasterKey GetNMTokenMasterKey();

		void SetNMTokenMasterKey(MasterKey secretKey);

		NodeAction GetNodeAction();

		void SetNodeAction(NodeAction nodeAction);

		long GetRMIdentifier();

		void SetRMIdentifier(long rmIdentifier);

		string GetDiagnosticsMessage();

		void SetDiagnosticsMessage(string diagnosticsMessage);

		void SetRMVersion(string version);

		string GetRMVersion();
	}
}
