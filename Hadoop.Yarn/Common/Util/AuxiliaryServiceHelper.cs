using System.Collections.Generic;
using Org.Apache.Commons.Codec.Binary;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	public class AuxiliaryServiceHelper
	{
		public const string NmAuxService = "NM_AUX_SERVICE_";

		public static ByteBuffer GetServiceDataFromEnv(string serviceName, IDictionary<string
			, string> env)
		{
			string meta = env[GetPrefixServiceName(serviceName)];
			if (null == meta)
			{
				return null;
			}
			byte[] metaData = Base64.DecodeBase64(meta);
			return ByteBuffer.Wrap(metaData);
		}

		public static void SetServiceDataIntoEnv(string serviceName, ByteBuffer metaData, 
			IDictionary<string, string> env)
		{
			byte[] byteData = ((byte[])metaData.Array());
			env[GetPrefixServiceName(serviceName)] = Base64.EncodeBase64String(byteData);
		}

		private static string GetPrefixServiceName(string serviceName)
		{
			return NmAuxService + serviceName;
		}
	}
}
