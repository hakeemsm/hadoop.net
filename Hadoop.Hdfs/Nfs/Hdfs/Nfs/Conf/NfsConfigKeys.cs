using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Conf
{
	public class NfsConfigKeys
	{
		public const string DfsNfsServerPortKey = "nfs.server.port";

		public const int DfsNfsServerPortDefault = 2049;

		public const string DfsNfsMountdPortKey = "nfs.mountd.port";

		public const int DfsNfsMountdPortDefault = 4242;

		public const string DfsNfsFileDumpKey = "nfs.file.dump";

		public const bool DfsNfsFileDumpDefault = true;

		public const string DfsNfsFileDumpDirKey = "nfs.file.dump.dir";

		public const string DfsNfsFileDumpDirDefault = "/tmp/.hdfs-nfs";

		public const string DfsNfsMaxReadTransferSizeKey = "nfs.rtmax";

		public const int DfsNfsMaxReadTransferSizeDefault = 1024 * 1024;

		public const string DfsNfsMaxWriteTransferSizeKey = "nfs.wtmax";

		public const int DfsNfsMaxWriteTransferSizeDefault = 1024 * 1024;

		public const string DfsNfsMaxReaddirTransferSizeKey = "nfs.dtmax";

		public const int DfsNfsMaxReaddirTransferSizeDefault = 64 * 1024;

		public const string DfsNfsMaxOpenFilesKey = "nfs.max.open.files";

		public const int DfsNfsMaxOpenFilesDefault = 256;

		public const string DfsNfsStreamTimeoutKey = "nfs.stream.timeout";

		public const long DfsNfsStreamTimeoutDefault = 10 * 60 * 1000;

		public const long DfsNfsStreamTimeoutMinDefault = 10 * 1000;

		public const string DfsNfsExportPointKey = "nfs.export.point";

		public const string DfsNfsExportPointDefault = "/";

		public const string DfsNfsKeytabFileKey = "nfs.keytab.file";

		public const string DfsNfsKerberosPrincipalKey = "nfs.kerberos.principal";

		public const string DfsNfsRegistrationPortKey = "nfs.registration.port";

		public const int DfsNfsRegistrationPortDefault = 40;

		public const string DfsNfsPortMonitoringDisabledKey = "nfs.port.monitoring.disabled";

		public const bool DfsNfsPortMonitoringDisabledDefault = true;

		public const string AixCompatModeKey = "nfs.aix.compatibility.mode.enabled";

		public const bool AixCompatModeDefault = false;

		public const string LargeFileUpload = "nfs.large.file.upload";

		public const bool LargeFileUploadDefault = true;

		public const string NfsHttpPortKey = "nfs.http.port";

		public const int NfsHttpPortDefault = 50079;

		public const string NfsHttpAddressKey = "nfs.http.address";

		public const string NfsHttpAddressDefault = "0.0.0.0:" + NfsHttpPortDefault;

		public const string NfsHttpsPortKey = "nfs.https.port";

		public const int NfsHttpsPortDefault = 50579;

		public const string NfsHttpsAddressKey = "nfs.https.address";

		public const string NfsHttpsAddressDefault = "0.0.0.0:" + NfsHttpsPortDefault;

		public const string NfsMetricsPercentilesIntervalsKey = "nfs.metrics.percentiles.intervals";

		public const string NfsSuperuserKey = "nfs.superuser";

		public const string NfsSuperuserDefault = string.Empty;
		// The IP port number for NFS and mountd.
		// 10 minutes
		// 10 seconds
		// Currently unassigned.
		/*
		* HDFS super-user is the user with the same identity as NameNode process
		* itself and the super-user can do anything in that permissions checks never
		* fail for the super-user. If the following property is configured, the
		* superuser on NFS client can access any file on HDFS. By default, the super
		* user is not configured in the gateway. Note that, even the the superuser is
		* configured, "nfs.exports.allowed.hosts" still takes effect. For example,
		* the superuser will not have write access to HDFS files through the gateway
		* if the NFS client host is not allowed to have write access in
		* "nfs.exports.allowed.hosts".
		*/
	}
}
