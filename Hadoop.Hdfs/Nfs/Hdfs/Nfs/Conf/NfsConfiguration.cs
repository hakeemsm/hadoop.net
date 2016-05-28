using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Conf
{
	/// <summary>Adds deprecated keys into the configuration.</summary>
	public class NfsConfiguration : HdfsConfiguration
	{
		static NfsConfiguration()
		{
			AddDeprecatedKeys();
		}

		private static void AddDeprecatedKeys()
		{
			Configuration.AddDeprecations(new Configuration.DeprecationDelta[] { new Configuration.DeprecationDelta
				("nfs3.server.port", NfsConfigKeys.DfsNfsServerPortKey), new Configuration.DeprecationDelta
				("nfs3.mountd.port", NfsConfigKeys.DfsNfsMountdPortKey), new Configuration.DeprecationDelta
				("dfs.nfs.exports.cache.size", Nfs3Constant.NfsExportsCacheSizeKey), new Configuration.DeprecationDelta
				("dfs.nfs.exports.cache.expirytime.millis", Nfs3Constant.NfsExportsCacheExpirytimeMillisKey
				), new Configuration.DeprecationDelta("hadoop.nfs.userupdate.milly", IdMappingConstant
				.UsergroupidUpdateMillisKey), new Configuration.DeprecationDelta("nfs.usergroup.update.millis"
				, IdMappingConstant.UsergroupidUpdateMillisKey), new Configuration.DeprecationDelta
				("nfs.static.mapping.file", IdMappingConstant.StaticIdMappingFileKey), new Configuration.DeprecationDelta
				("dfs.nfs3.enableDump", NfsConfigKeys.DfsNfsFileDumpKey), new Configuration.DeprecationDelta
				("dfs.nfs3.dump.dir", NfsConfigKeys.DfsNfsFileDumpDirKey), new Configuration.DeprecationDelta
				("dfs.nfs3.max.open.files", NfsConfigKeys.DfsNfsMaxOpenFilesKey), new Configuration.DeprecationDelta
				("dfs.nfs3.stream.timeout", NfsConfigKeys.DfsNfsStreamTimeoutKey), new Configuration.DeprecationDelta
				("dfs.nfs3.export.point", NfsConfigKeys.DfsNfsExportPointKey), new Configuration.DeprecationDelta
				("nfs.allow.insecure.ports", NfsConfigKeys.DfsNfsPortMonitoringDisabledKey), new 
				Configuration.DeprecationDelta("dfs.nfs.keytab.file", NfsConfigKeys.DfsNfsKeytabFileKey
				), new Configuration.DeprecationDelta("dfs.nfs.kerberos.principal", NfsConfigKeys
				.DfsNfsKerberosPrincipalKey), new Configuration.DeprecationDelta("dfs.nfs.rtmax"
				, NfsConfigKeys.DfsNfsMaxReadTransferSizeKey), new Configuration.DeprecationDelta
				("dfs.nfs.wtmax", NfsConfigKeys.DfsNfsMaxWriteTransferSizeKey), new Configuration.DeprecationDelta
				("dfs.nfs.dtmax", NfsConfigKeys.DfsNfsMaxReaddirTransferSizeKey) });
		}
	}
}
