using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Lib.Server;
using Org.Apache.Hadoop.Lib.Service;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Org.Apache.Hadoop.Test;
using Org.Json.Simple;
using Org.Json.Simple.Parser;
using Org.Mortbay.Jetty.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Server
{
	public class TestHttpFSServer : HFSTestCase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		public virtual void Server()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			Configuration httpfsConf = new Configuration(false);
			HttpFSServerWebApp server = new HttpFSServerWebApp(dir, dir, dir, dir, httpfsConf
				);
			server.Init();
			server.Destroy();
		}

		public class MockGroups : Service, Groups
		{
			/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
			public virtual void Init(Org.Apache.Hadoop.Lib.Server.Server server)
			{
			}

			/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
			public virtual void PostInit()
			{
			}

			public virtual void Destroy()
			{
			}

			public virtual Type[] GetServiceDependencies()
			{
				return new Type[0];
			}

			public virtual Type GetInterface()
			{
				return typeof(Groups);
			}

			/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
			public virtual void ServerStatusChange(Server.Status oldStatus, Server.Status newStatus
				)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual IList<string> GetGroups(string user)
			{
				return Arrays.AsList(HadoopUsersConfTestHelper.GetHadoopUserGroups(user));
			}
		}

		/// <exception cref="System.Exception"/>
		private void CreateHttpFSServer(bool addDelegationTokenAuthHandler)
		{
			FilePath homeDir = TestDirHelper.GetTestDir();
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "conf").Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "log").Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "temp").Mkdir());
			HttpFSServerWebApp.SetHomeDirForCurrentThread(homeDir.GetAbsolutePath());
			FilePath secretFile = new FilePath(new FilePath(homeDir, "conf"), "secret");
			TextWriter w = new FileWriter(secretFile);
			w.Write("secret");
			w.Close();
			//HDFS configuration
			FilePath hadoopConfDir = new FilePath(new FilePath(homeDir, "conf"), "hadoop-conf"
				);
			hadoopConfDir.Mkdirs();
			string fsDefaultName = TestHdfsHelper.GetHdfsConf().Get(CommonConfigurationKeysPublic
				.FsDefaultNameKey);
			Configuration conf = new Configuration(false);
			conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, fsDefaultName);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeXattrsEnabledKey, true);
			FilePath hdfsSite = new FilePath(hadoopConfDir, "hdfs-site.xml");
			OutputStream os = new FileOutputStream(hdfsSite);
			conf.WriteXml(os);
			os.Close();
			//HTTPFS configuration
			conf = new Configuration(false);
			if (addDelegationTokenAuthHandler)
			{
				conf.Set("httpfs.authentication.type", typeof(HttpFSKerberosAuthenticationHandlerForTesting
					).FullName);
			}
			conf.Set("httpfs.services.ext", typeof(TestHttpFSServer.MockGroups).FullName);
			conf.Set("httpfs.admin.group", HadoopUsersConfTestHelper.GetHadoopUserGroups(HadoopUsersConfTestHelper
				.GetHadoopUsers()[0])[0]);
			conf.Set("httpfs.proxyuser." + HadoopUsersConfTestHelper.GetHadoopProxyUser() + ".groups"
				, HadoopUsersConfTestHelper.GetHadoopProxyUserGroups());
			conf.Set("httpfs.proxyuser." + HadoopUsersConfTestHelper.GetHadoopProxyUser() + ".hosts"
				, HadoopUsersConfTestHelper.GetHadoopProxyUserHosts());
			conf.Set("httpfs.authentication.signature.secret.file", secretFile.GetAbsolutePath
				());
			conf.Set("httpfs.hadoop.config.dir", hadoopConfDir.ToString());
			FilePath httpfsSite = new FilePath(new FilePath(homeDir, "conf"), "httpfs-site.xml"
				);
			os = new FileOutputStream(httpfsSite);
			conf.WriteXml(os);
			os.Close();
			ClassLoader cl = Sharpen.Thread.CurrentThread().GetContextClassLoader();
			Uri url = cl.GetResource("webapp");
			WebAppContext context = new WebAppContext(url.AbsolutePath, "/webhdfs");
			Org.Mortbay.Jetty.Server server = TestJettyHelper.GetJettyServer();
			server.AddHandler(context);
			server.Start();
			if (addDelegationTokenAuthHandler)
			{
				HttpFSServerWebApp.Get().SetAuthority(TestJettyHelper.GetAuthority());
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void Instrumentation()
		{
			CreateHttpFSServer(false);
			Uri url = new Uri(TestJettyHelper.GetJettyURL(), MessageFormat.Format("/webhdfs/v1?user.name={0}&op=instrumentation"
				, "nobody"));
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			NUnit.Framework.Assert.AreEqual(conn.GetResponseCode(), HttpURLConnection.HttpUnauthorized
				);
			url = new Uri(TestJettyHelper.GetJettyURL(), MessageFormat.Format("/webhdfs/v1?user.name={0}&op=instrumentation"
				, HadoopUsersConfTestHelper.GetHadoopUsers()[0]));
			conn = (HttpURLConnection)url.OpenConnection();
			NUnit.Framework.Assert.AreEqual(conn.GetResponseCode(), HttpURLConnection.HttpOk);
			BufferedReader reader = new BufferedReader(new InputStreamReader(conn.GetInputStream
				()));
			string line = reader.ReadLine();
			reader.Close();
			NUnit.Framework.Assert.IsTrue(line.Contains("\"counters\":{"));
			url = new Uri(TestJettyHelper.GetJettyURL(), MessageFormat.Format("/webhdfs/v1/foo?user.name={0}&op=instrumentation"
				, HadoopUsersConfTestHelper.GetHadoopUsers()[0]));
			conn = (HttpURLConnection)url.OpenConnection();
			NUnit.Framework.Assert.AreEqual(conn.GetResponseCode(), HttpURLConnection.HttpBadRequest
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void TestHdfsAccess()
		{
			CreateHttpFSServer(false);
			string user = HadoopUsersConfTestHelper.GetHadoopUsers()[0];
			Uri url = new Uri(TestJettyHelper.GetJettyURL(), MessageFormat.Format("/webhdfs/v1/?user.name={0}&op=liststatus"
				, user));
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			NUnit.Framework.Assert.AreEqual(conn.GetResponseCode(), HttpURLConnection.HttpOk);
			BufferedReader reader = new BufferedReader(new InputStreamReader(conn.GetInputStream
				()));
			reader.ReadLine();
			reader.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void TestGlobFilter()
		{
			CreateHttpFSServer(false);
			FileSystem fs = FileSystem.Get(TestHdfsHelper.GetHdfsConf());
			fs.Mkdirs(new Path("/tmp"));
			fs.Create(new Path("/tmp/foo.txt")).Close();
			string user = HadoopUsersConfTestHelper.GetHadoopUsers()[0];
			Uri url = new Uri(TestJettyHelper.GetJettyURL(), MessageFormat.Format("/webhdfs/v1/tmp?user.name={0}&op=liststatus&filter=f*"
				, user));
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			NUnit.Framework.Assert.AreEqual(conn.GetResponseCode(), HttpURLConnection.HttpOk);
			BufferedReader reader = new BufferedReader(new InputStreamReader(conn.GetInputStream
				()));
			reader.ReadLine();
			reader.Close();
		}

		/// <summary>Talks to the http interface to create a file.</summary>
		/// <param name="filename">The file to create</param>
		/// <param name="perms">The permission field, if any (may be null)</param>
		/// <exception cref="System.Exception"/>
		private void CreateWithHttp(string filename, string perms)
		{
			string user = HadoopUsersConfTestHelper.GetHadoopUsers()[0];
			// Remove leading / from filename
			if (filename[0] == '/')
			{
				filename = Sharpen.Runtime.Substring(filename, 1);
			}
			string pathOps;
			if (perms == null)
			{
				pathOps = MessageFormat.Format("/webhdfs/v1/{0}?user.name={1}&op=CREATE", filename
					, user);
			}
			else
			{
				pathOps = MessageFormat.Format("/webhdfs/v1/{0}?user.name={1}&permission={2}&op=CREATE"
					, filename, user, perms);
			}
			Uri url = new Uri(TestJettyHelper.GetJettyURL(), pathOps);
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			conn.AddRequestProperty("Content-Type", "application/octet-stream");
			conn.SetRequestMethod("PUT");
			conn.Connect();
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpCreated, conn.GetResponseCode
				());
		}

		/// <summary>
		/// Talks to the http interface to get the json output of a *STATUS command
		/// on the given file.
		/// </summary>
		/// <param name="filename">The file to query.</param>
		/// <param name="command">Either GETFILESTATUS, LISTSTATUS, or ACLSTATUS</param>
		/// <returns>A string containing the JSON output describing the file.</returns>
		/// <exception cref="System.Exception"/>
		private string GetStatus(string filename, string command)
		{
			string user = HadoopUsersConfTestHelper.GetHadoopUsers()[0];
			// Remove leading / from filename
			if (filename[0] == '/')
			{
				filename = Sharpen.Runtime.Substring(filename, 1);
			}
			string pathOps = MessageFormat.Format("/webhdfs/v1/{0}?user.name={1}&op={2}", filename
				, user, command);
			Uri url = new Uri(TestJettyHelper.GetJettyURL(), pathOps);
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			conn.Connect();
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
			BufferedReader reader = new BufferedReader(new InputStreamReader(conn.GetInputStream
				()));
			return reader.ReadLine();
		}

		/// <summary>General-purpose http PUT command to the httpfs server.</summary>
		/// <param name="filename">The file to operate upon</param>
		/// <param name="command">The command to perform (SETACL, etc)</param>
		/// <param name="params">Parameters, like "aclspec=..."</param>
		/// <exception cref="System.Exception"/>
		private void PutCmd(string filename, string command, string @params)
		{
			string user = HadoopUsersConfTestHelper.GetHadoopUsers()[0];
			// Remove leading / from filename
			if (filename[0] == '/')
			{
				filename = Sharpen.Runtime.Substring(filename, 1);
			}
			string pathOps = MessageFormat.Format("/webhdfs/v1/{0}?user.name={1}{2}{3}&op={4}"
				, filename, user, (@params == null) ? string.Empty : "&", (@params == null) ? string.Empty
				 : @params, command);
			Uri url = new Uri(TestJettyHelper.GetJettyURL(), pathOps);
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			conn.SetRequestMethod("PUT");
			conn.Connect();
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
		}

		/// <summary>
		/// Given the JSON output from the GETFILESTATUS call, return the
		/// 'permission' value.
		/// </summary>
		/// <param name="statusJson">JSON from GETFILESTATUS</param>
		/// <returns>The value of 'permission' in statusJson</returns>
		/// <exception cref="System.Exception"/>
		private string GetPerms(string statusJson)
		{
			JSONParser parser = new JSONParser();
			JSONObject jsonObject = (JSONObject)parser.Parse(statusJson);
			JSONObject details = (JSONObject)jsonObject["FileStatus"];
			return (string)details["permission"];
		}

		/// <summary>
		/// Given the JSON output from the GETACLSTATUS call, return the
		/// 'entries' value as a List<String>.
		/// </summary>
		/// <param name="statusJson">JSON from GETACLSTATUS</param>
		/// <returns>A List of Strings which are the elements of the ACL entries</returns>
		/// <exception cref="System.Exception"/>
		private IList<string> GetAclEntries(string statusJson)
		{
			IList<string> entries = new AList<string>();
			JSONParser parser = new JSONParser();
			JSONObject jsonObject = (JSONObject)parser.Parse(statusJson);
			JSONObject details = (JSONObject)jsonObject["AclStatus"];
			JSONArray jsonEntries = (JSONArray)details["entries"];
			if (jsonEntries != null)
			{
				foreach (object e in jsonEntries)
				{
					entries.AddItem(e.ToString());
				}
			}
			return entries;
		}

		/// <summary>Parse xAttrs from JSON result of GETXATTRS call, return xAttrs Map.</summary>
		/// <param name="statusJson">JSON from GETXATTRS</param>
		/// <returns>Map<String, byte[]> xAttrs Map</returns>
		/// <exception cref="System.Exception"/>
		private IDictionary<string, byte[]> GetXAttrs(string statusJson)
		{
			IDictionary<string, byte[]> xAttrs = Maps.NewHashMap();
			JSONParser parser = new JSONParser();
			JSONObject jsonObject = (JSONObject)parser.Parse(statusJson);
			JSONArray jsonXAttrs = (JSONArray)jsonObject["XAttrs"];
			if (jsonXAttrs != null)
			{
				foreach (object a in jsonXAttrs)
				{
					string name = (string)((JSONObject)a)["name"];
					string value = (string)((JSONObject)a)["value"];
					xAttrs[name] = DecodeXAttrValue(value);
				}
			}
			return xAttrs;
		}

		/// <summary>Decode xattr value from string</summary>
		/// <exception cref="System.IO.IOException"/>
		private byte[] DecodeXAttrValue(string value)
		{
			if (value != null)
			{
				return XAttrCodec.DecodeValue(value);
			}
			else
			{
				return new byte[0];
			}
		}

		/// <summary>
		/// Validate that files are created with 755 permissions when no
		/// 'permissions' attribute is specified, and when 'permissions'
		/// is specified, that value is honored.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void TestPerms()
		{
			CreateHttpFSServer(false);
			FileSystem fs = FileSystem.Get(TestHdfsHelper.GetHdfsConf());
			fs.Mkdirs(new Path("/perm"));
			CreateWithHttp("/perm/none", null);
			string statusJson = GetStatus("/perm/none", "GETFILESTATUS");
			NUnit.Framework.Assert.IsTrue("755".Equals(GetPerms(statusJson)));
			CreateWithHttp("/perm/p-777", "777");
			statusJson = GetStatus("/perm/p-777", "GETFILESTATUS");
			NUnit.Framework.Assert.IsTrue("777".Equals(GetPerms(statusJson)));
			CreateWithHttp("/perm/p-654", "654");
			statusJson = GetStatus("/perm/p-654", "GETFILESTATUS");
			NUnit.Framework.Assert.IsTrue("654".Equals(GetPerms(statusJson)));
			CreateWithHttp("/perm/p-321", "321");
			statusJson = GetStatus("/perm/p-321", "GETFILESTATUS");
			NUnit.Framework.Assert.IsTrue("321".Equals(GetPerms(statusJson)));
		}

		/// <summary>Validate XAttr get/set/remove calls.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void TestXAttrs()
		{
			string name1 = "user.a1";
			byte[] value1 = new byte[] { unchecked((int)(0x31)), unchecked((int)(0x32)), unchecked(
				(int)(0x33)) };
			string name2 = "user.a2";
			byte[] value2 = new byte[] { unchecked((int)(0x41)), unchecked((int)(0x42)), unchecked(
				(int)(0x43)) };
			string dir = "/xattrTest";
			string path = dir + "/file";
			CreateHttpFSServer(false);
			FileSystem fs = FileSystem.Get(TestHdfsHelper.GetHdfsConf());
			fs.Mkdirs(new Path(dir));
			CreateWithHttp(path, null);
			string statusJson = GetStatus(path, "GETXATTRS");
			IDictionary<string, byte[]> xAttrs = GetXAttrs(statusJson);
			NUnit.Framework.Assert.AreEqual(0, xAttrs.Count);
			// Set two xattrs
			PutCmd(path, "SETXATTR", SetXAttrParam(name1, value1));
			PutCmd(path, "SETXATTR", SetXAttrParam(name2, value2));
			statusJson = GetStatus(path, "GETXATTRS");
			xAttrs = GetXAttrs(statusJson);
			NUnit.Framework.Assert.AreEqual(2, xAttrs.Count);
			Assert.AssertArrayEquals(value1, xAttrs[name1]);
			Assert.AssertArrayEquals(value2, xAttrs[name2]);
			// Remove one xattr
			PutCmd(path, "REMOVEXATTR", "xattr.name=" + name1);
			statusJson = GetStatus(path, "GETXATTRS");
			xAttrs = GetXAttrs(statusJson);
			NUnit.Framework.Assert.AreEqual(1, xAttrs.Count);
			Assert.AssertArrayEquals(value2, xAttrs[name2]);
			// Remove another xattr, then there is no xattr
			PutCmd(path, "REMOVEXATTR", "xattr.name=" + name2);
			statusJson = GetStatus(path, "GETXATTRS");
			xAttrs = GetXAttrs(statusJson);
			NUnit.Framework.Assert.AreEqual(0, xAttrs.Count);
		}

		/// <summary>Params for setting an xAttr</summary>
		/// <exception cref="System.IO.IOException"/>
		public static string SetXAttrParam(string name, byte[] value)
		{
			return "xattr.name=" + name + "&xattr.value=" + XAttrCodec.EncodeValue(value, XAttrCodec
				.Hex) + "&encoding=hex&flag=create";
		}

		/// <summary>Validate the various ACL set/modify/remove calls.</summary>
		/// <remarks>
		/// Validate the various ACL set/modify/remove calls.  General strategy is
		/// to verify each of the following steps with GETFILESTATUS, LISTSTATUS,
		/// and GETACLSTATUS:
		/// <ol>
		/// <li>Create a file with no ACLs</li>
		/// <li>Add a user + group ACL</li>
		/// <li>Add another user ACL</li>
		/// <li>Remove the first user ACL</li>
		/// <li>Remove all ACLs</li>
		/// </ol>
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void TestFileAcls()
		{
			string aclUser1 = "user:foo:rw-";
			string aclUser2 = "user:bar:r--";
			string aclGroup1 = "group::r--";
			string aclSpec = "aclspec=user::rwx," + aclUser1 + "," + aclGroup1 + ",other::---";
			string modAclSpec = "aclspec=" + aclUser2;
			string remAclSpec = "aclspec=" + aclUser1;
			string dir = "/aclFileTest";
			string path = dir + "/test";
			string statusJson;
			IList<string> aclEntries;
			CreateHttpFSServer(false);
			FileSystem fs = FileSystem.Get(TestHdfsHelper.GetHdfsConf());
			fs.Mkdirs(new Path(dir));
			CreateWithHttp(path, null);
			/* getfilestatus and liststatus don't have 'aclBit' in their reply */
			statusJson = GetStatus(path, "GETFILESTATUS");
			NUnit.Framework.Assert.AreEqual(-1, statusJson.IndexOf("aclBit"));
			statusJson = GetStatus(dir, "LISTSTATUS");
			NUnit.Framework.Assert.AreEqual(-1, statusJson.IndexOf("aclBit"));
			/* getaclstatus works and returns no entries */
			statusJson = GetStatus(path, "GETACLSTATUS");
			aclEntries = GetAclEntries(statusJson);
			NUnit.Framework.Assert.IsTrue(aclEntries.Count == 0);
			/*
			* Now set an ACL on the file.  (getfile|list)status have aclBit,
			* and aclstatus has entries that looks familiar.
			*/
			PutCmd(path, "SETACL", aclSpec);
			statusJson = GetStatus(path, "GETFILESTATUS");
			Assert.AssertNotEquals(-1, statusJson.IndexOf("aclBit"));
			statusJson = GetStatus(dir, "LISTSTATUS");
			Assert.AssertNotEquals(-1, statusJson.IndexOf("aclBit"));
			statusJson = GetStatus(path, "GETACLSTATUS");
			aclEntries = GetAclEntries(statusJson);
			NUnit.Framework.Assert.IsTrue(aclEntries.Count == 2);
			NUnit.Framework.Assert.IsTrue(aclEntries.Contains(aclUser1));
			NUnit.Framework.Assert.IsTrue(aclEntries.Contains(aclGroup1));
			/* Modify acl entries to add another user acl */
			PutCmd(path, "MODIFYACLENTRIES", modAclSpec);
			statusJson = GetStatus(path, "GETACLSTATUS");
			aclEntries = GetAclEntries(statusJson);
			NUnit.Framework.Assert.IsTrue(aclEntries.Count == 3);
			NUnit.Framework.Assert.IsTrue(aclEntries.Contains(aclUser1));
			NUnit.Framework.Assert.IsTrue(aclEntries.Contains(aclUser2));
			NUnit.Framework.Assert.IsTrue(aclEntries.Contains(aclGroup1));
			/* Remove the first user acl entry and verify */
			PutCmd(path, "REMOVEACLENTRIES", remAclSpec);
			statusJson = GetStatus(path, "GETACLSTATUS");
			aclEntries = GetAclEntries(statusJson);
			NUnit.Framework.Assert.IsTrue(aclEntries.Count == 2);
			NUnit.Framework.Assert.IsTrue(aclEntries.Contains(aclUser2));
			NUnit.Framework.Assert.IsTrue(aclEntries.Contains(aclGroup1));
			/* Remove all acls and verify */
			PutCmd(path, "REMOVEACL", null);
			statusJson = GetStatus(path, "GETACLSTATUS");
			aclEntries = GetAclEntries(statusJson);
			NUnit.Framework.Assert.IsTrue(aclEntries.Count == 0);
			statusJson = GetStatus(path, "GETFILESTATUS");
			NUnit.Framework.Assert.AreEqual(-1, statusJson.IndexOf("aclBit"));
			statusJson = GetStatus(dir, "LISTSTATUS");
			NUnit.Framework.Assert.AreEqual(-1, statusJson.IndexOf("aclBit"));
		}

		/// <summary>Test ACL operations on a directory, including default ACLs.</summary>
		/// <remarks>
		/// Test ACL operations on a directory, including default ACLs.
		/// General strategy is to use GETFILESTATUS and GETACLSTATUS to verify:
		/// <ol>
		/// <li>Initial status with no ACLs</li>
		/// <li>The addition of a default ACL</li>
		/// <li>The removal of default ACLs</li>
		/// </ol>
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void TestDirAcls()
		{
			string defUser1 = "default:user:glarch:r-x";
			string defSpec1 = "aclspec=" + defUser1;
			string dir = "/aclDirTest";
			string statusJson;
			IList<string> aclEntries;
			CreateHttpFSServer(false);
			FileSystem fs = FileSystem.Get(TestHdfsHelper.GetHdfsConf());
			fs.Mkdirs(new Path(dir));
			/* getfilestatus and liststatus don't have 'aclBit' in their reply */
			statusJson = GetStatus(dir, "GETFILESTATUS");
			NUnit.Framework.Assert.AreEqual(-1, statusJson.IndexOf("aclBit"));
			/* No ACLs, either */
			statusJson = GetStatus(dir, "GETACLSTATUS");
			aclEntries = GetAclEntries(statusJson);
			NUnit.Framework.Assert.IsTrue(aclEntries.Count == 0);
			/* Give it a default ACL and verify */
			PutCmd(dir, "SETACL", defSpec1);
			statusJson = GetStatus(dir, "GETFILESTATUS");
			Assert.AssertNotEquals(-1, statusJson.IndexOf("aclBit"));
			statusJson = GetStatus(dir, "GETACLSTATUS");
			aclEntries = GetAclEntries(statusJson);
			NUnit.Framework.Assert.IsTrue(aclEntries.Count == 5);
			/* 4 Entries are default:(user|group|mask|other):perm */
			NUnit.Framework.Assert.IsTrue(aclEntries.Contains(defUser1));
			/* Remove the default ACL and re-verify */
			PutCmd(dir, "REMOVEDEFAULTACL", null);
			statusJson = GetStatus(dir, "GETFILESTATUS");
			NUnit.Framework.Assert.AreEqual(-1, statusJson.IndexOf("aclBit"));
			statusJson = GetStatus(dir, "GETACLSTATUS");
			aclEntries = GetAclEntries(statusJson);
			NUnit.Framework.Assert.IsTrue(aclEntries.Count == 0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void TestOpenOffsetLength()
		{
			CreateHttpFSServer(false);
			byte[] array = new byte[] { 0, 1, 2, 3 };
			FileSystem fs = FileSystem.Get(TestHdfsHelper.GetHdfsConf());
			fs.Mkdirs(new Path("/tmp"));
			OutputStream os = fs.Create(new Path("/tmp/foo"));
			os.Write(array);
			os.Close();
			string user = HadoopUsersConfTestHelper.GetHadoopUsers()[0];
			Uri url = new Uri(TestJettyHelper.GetJettyURL(), MessageFormat.Format("/webhdfs/v1/tmp/foo?user.name={0}&op=open&offset=1&length=2"
				, user));
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
			InputStream @is = conn.GetInputStream();
			NUnit.Framework.Assert.AreEqual(1, @is.Read());
			NUnit.Framework.Assert.AreEqual(2, @is.Read());
			NUnit.Framework.Assert.AreEqual(-1, @is.Read());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void TestPutNoOperation()
		{
			CreateHttpFSServer(false);
			string user = HadoopUsersConfTestHelper.GetHadoopUsers()[0];
			Uri url = new Uri(TestJettyHelper.GetJettyURL(), MessageFormat.Format("/webhdfs/v1/foo?user.name={0}"
				, user));
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			conn.SetDoInput(true);
			conn.SetDoOutput(true);
			conn.SetRequestMethod("PUT");
			NUnit.Framework.Assert.AreEqual(conn.GetResponseCode(), HttpURLConnection.HttpBadRequest
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestJetty]
		[TestHdfs]
		public virtual void TestDelegationTokenOperations()
		{
			CreateHttpFSServer(true);
			Uri url = new Uri(TestJettyHelper.GetJettyURL(), "/webhdfs/v1/?op=GETHOMEDIRECTORY"
				);
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpUnauthorized, conn.GetResponseCode
				());
			AuthenticationToken token = new AuthenticationToken("u", "p", new KerberosDelegationTokenAuthenticationHandler
				().GetType());
			token.SetExpires(Runtime.CurrentTimeMillis() + 100000000);
			SignerSecretProvider secretProvider = StringSignerSecretProviderCreator.NewStringSignerSecretProvider
				();
			Properties secretProviderProps = new Properties();
			secretProviderProps.SetProperty(AuthenticationFilter.SignatureSecret, "secret");
			secretProvider.Init(secretProviderProps, null, -1);
			Signer signer = new Signer(secretProvider);
			string tokenSigned = signer.Sign(token.ToString());
			url = new Uri(TestJettyHelper.GetJettyURL(), "/webhdfs/v1/?op=GETHOMEDIRECTORY");
			conn = (HttpURLConnection)url.OpenConnection();
			conn.SetRequestProperty("Cookie", AuthenticatedURL.AuthCookie + "=" + tokenSigned
				);
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
			url = new Uri(TestJettyHelper.GetJettyURL(), "/webhdfs/v1/?op=GETDELEGATIONTOKEN"
				);
			conn = (HttpURLConnection)url.OpenConnection();
			conn.SetRequestProperty("Cookie", AuthenticatedURL.AuthCookie + "=" + tokenSigned
				);
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
			JSONObject json = (JSONObject)new JSONParser().Parse(new InputStreamReader(conn.GetInputStream
				()));
			json = (JSONObject)json[DelegationTokenAuthenticator.DelegationTokenJson];
			string tokenStr = (string)json[DelegationTokenAuthenticator.DelegationTokenUrlStringJson
				];
			url = new Uri(TestJettyHelper.GetJettyURL(), "/webhdfs/v1/?op=GETHOMEDIRECTORY&delegation="
				 + tokenStr);
			conn = (HttpURLConnection)url.OpenConnection();
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
			url = new Uri(TestJettyHelper.GetJettyURL(), "/webhdfs/v1/?op=RENEWDELEGATIONTOKEN&token="
				 + tokenStr);
			conn = (HttpURLConnection)url.OpenConnection();
			conn.SetRequestMethod("PUT");
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpUnauthorized, conn.GetResponseCode
				());
			url = new Uri(TestJettyHelper.GetJettyURL(), "/webhdfs/v1/?op=RENEWDELEGATIONTOKEN&token="
				 + tokenStr);
			conn = (HttpURLConnection)url.OpenConnection();
			conn.SetRequestMethod("PUT");
			conn.SetRequestProperty("Cookie", AuthenticatedURL.AuthCookie + "=" + tokenSigned
				);
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
			url = new Uri(TestJettyHelper.GetJettyURL(), "/webhdfs/v1/?op=CANCELDELEGATIONTOKEN&token="
				 + tokenStr);
			conn = (HttpURLConnection)url.OpenConnection();
			conn.SetRequestMethod("PUT");
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
			url = new Uri(TestJettyHelper.GetJettyURL(), "/webhdfs/v1/?op=GETHOMEDIRECTORY&delegation="
				 + tokenStr);
			conn = (HttpURLConnection)url.OpenConnection();
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpForbidden, conn.GetResponseCode
				());
		}
	}
}
