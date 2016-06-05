using System;
using System.Collections.Generic;
using System.IO;
using Javax.Naming;
using Javax.Naming.Directory;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security.Alias;


namespace Org.Apache.Hadoop.Security
{
	public class TestLdapGroupsMapping
	{
		private DirContext mockContext;

		private LdapGroupsMapping mappingSpy = Org.Mockito.Mockito.Spy(new LdapGroupsMapping
			());

		private NamingEnumeration mockUserNamingEnum = Org.Mockito.Mockito.Mock<NamingEnumeration
			>();

		private NamingEnumeration mockGroupNamingEnum = Org.Mockito.Mockito.Mock<NamingEnumeration
			>();

		private string[] testGroups = new string[] { "group1", "group2" };

		/// <exception cref="Javax.Naming.NamingException"/>
		[SetUp]
		public virtual void SetupMocks()
		{
			mockContext = Org.Mockito.Mockito.Mock<DirContext>();
			Org.Mockito.Mockito.DoReturn(mockContext).When(mappingSpy).GetDirContext();
			SearchResult mockUserResult = Org.Mockito.Mockito.Mock<SearchResult>();
			// We only ever call hasMoreElements once for the user NamingEnum, so 
			// we can just have one return value
			Org.Mockito.Mockito.When(mockUserNamingEnum.MoveNext()).ThenReturn(true);
			Org.Mockito.Mockito.When(mockUserNamingEnum.Current).ThenReturn(mockUserResult);
			Org.Mockito.Mockito.When(mockUserResult.GetNameInNamespace()).ThenReturn("CN=some_user,DC=test,DC=com"
				);
			SearchResult mockGroupResult = Org.Mockito.Mockito.Mock<SearchResult>();
			// We're going to have to define the loop here. We want two iterations,
			// to get both the groups
			Org.Mockito.Mockito.When(mockGroupNamingEnum.MoveNext()).ThenReturn(true, true, false
				);
			Org.Mockito.Mockito.When(mockGroupNamingEnum.Current).ThenReturn(mockGroupResult);
			// Define the attribute for the name of the first group
			Attribute group1Attr = new BasicAttribute("cn");
			group1Attr.Add(testGroups[0]);
			Attributes group1Attrs = new BasicAttributes();
			group1Attrs.Put(group1Attr);
			// Define the attribute for the name of the second group
			Attribute group2Attr = new BasicAttribute("cn");
			group2Attr.Add(testGroups[1]);
			Attributes group2Attrs = new BasicAttributes();
			group2Attrs.Put(group2Attr);
			// This search result gets reused, so return group1, then group2
			Org.Mockito.Mockito.When(mockGroupResult.GetAttributes()).ThenReturn(group1Attrs, 
				group2Attrs);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Naming.NamingException"/>
		[Fact]
		public virtual void TestGetGroups()
		{
			// The search functionality of the mock context is reused, so we will
			// return the user NamingEnumeration first, and then the group
			Org.Mockito.Mockito.When(mockContext.Search(AnyString(), AnyString(), Any<object[]
				>(), Any<SearchControls>())).ThenReturn(mockUserNamingEnum, mockGroupNamingEnum);
			DoTestGetGroups(Arrays.AsList(testGroups), 2);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Naming.NamingException"/>
		[Fact]
		public virtual void TestGetGroupsWithConnectionClosed()
		{
			// The case mocks connection is closed/gc-ed, so the first search call throws CommunicationException,
			// then after reconnected return the user NamingEnumeration first, and then the group
			Org.Mockito.Mockito.When(mockContext.Search(AnyString(), AnyString(), Any<object[]
				>(), Any<SearchControls>())).ThenThrow(new CommunicationException("Connection is closed"
				)).ThenReturn(mockUserNamingEnum, mockGroupNamingEnum);
			// Although connection is down but after reconnected it still should retrieve the result groups
			DoTestGetGroups(Arrays.AsList(testGroups), 1 + 2);
		}

		// 1 is the first failure call 
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Naming.NamingException"/>
		[Fact]
		public virtual void TestGetGroupsWithLdapDown()
		{
			// This mocks the case where Ldap server is down, and always throws CommunicationException 
			Org.Mockito.Mockito.When(mockContext.Search(AnyString(), AnyString(), Any<object[]
				>(), Any<SearchControls>())).ThenThrow(new CommunicationException("Connection is closed"
				));
			// Ldap server is down, no groups should be retrieved
			DoTestGetGroups(Arrays.AsList(new string[] {  }), 1 + LdapGroupsMapping.ReconnectRetryCount
				);
		}

		// 1 is the first normal call
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Naming.NamingException"/>
		private void DoTestGetGroups(IList<string> expectedGroups, int searchTimes)
		{
			Configuration conf = new Configuration();
			// Set this, so we don't throw an exception
			conf.Set(LdapGroupsMapping.LdapUrlKey, "ldap://test");
			mappingSpy.SetConf(conf);
			// Username is arbitrary, since the spy is mocked to respond the same,
			// regardless of input
			IList<string> groups = mappingSpy.GetGroups("some_user");
			Assert.Equal(expectedGroups, groups);
			// We should have searched for a user, and then two groups
			Org.Mockito.Mockito.Verify(mockContext, Org.Mockito.Mockito.Times(searchTimes)).Search
				(AnyString(), AnyString(), Any<object[]>(), Any<SearchControls>());
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestExtractPassword()
		{
			FilePath testDir = new FilePath(Runtime.GetProperty("test.build.data", "target/test-dir"
				));
			testDir.Mkdirs();
			FilePath secretFile = new FilePath(testDir, "secret.txt");
			TextWriter writer = new FileWriter(secretFile);
			writer.Write("hadoop");
			writer.Close();
			LdapGroupsMapping mapping = new LdapGroupsMapping();
			Assert.Equal("hadoop", mapping.ExtractPassword(secretFile.GetPath
				()));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestConfGetPassword()
		{
			FilePath testDir = new FilePath(Runtime.GetProperty("test.build.data", "target/test-dir"
				));
			Configuration conf = new Configuration();
			Path jksPath = new Path(testDir.ToString(), "test.jks");
			string ourUrl = JavaKeyStoreProvider.SchemeName + "://file" + jksPath.ToUri();
			FilePath file = new FilePath(testDir, "test.jks");
			file.Delete();
			conf.Set(CredentialProviderFactory.CredentialProviderPath, ourUrl);
			CredentialProvider provider = CredentialProviderFactory.GetProviders(conf)[0];
			char[] bindpass = new char[] { 'b', 'i', 'n', 'd', 'p', 'a', 's', 's' };
			char[] storepass = new char[] { 's', 't', 'o', 'r', 'e', 'p', 'a', 's', 's' };
			// ensure that we get nulls when the key isn't there
			Assert.Equal(null, provider.GetCredentialEntry(LdapGroupsMapping
				.BindPasswordKey));
			Assert.Equal(null, provider.GetCredentialEntry(LdapGroupsMapping
				.LdapKeystorePasswordKey));
			// create new aliases
			try
			{
				provider.CreateCredentialEntry(LdapGroupsMapping.BindPasswordKey, bindpass);
				provider.CreateCredentialEntry(LdapGroupsMapping.LdapKeystorePasswordKey, storepass
					);
				provider.Flush();
			}
			catch (Exception e)
			{
				Runtime.PrintStackTrace(e);
				throw;
			}
			// make sure we get back the right key
			Assert.AssertArrayEquals(bindpass, provider.GetCredentialEntry(LdapGroupsMapping.
				BindPasswordKey).GetCredential());
			Assert.AssertArrayEquals(storepass, provider.GetCredentialEntry(LdapGroupsMapping
				.LdapKeystorePasswordKey).GetCredential());
			LdapGroupsMapping mapping = new LdapGroupsMapping();
			Assert.Equal("bindpass", mapping.GetPassword(conf, LdapGroupsMapping
				.BindPasswordKey, string.Empty));
			Assert.Equal("storepass", mapping.GetPassword(conf, LdapGroupsMapping
				.LdapKeystorePasswordKey, string.Empty));
			// let's make sure that a password that doesn't exist returns an
			// empty string as currently expected and used to trigger a call to
			// extract password
			Assert.Equal(string.Empty, mapping.GetPassword(conf, "invalid-alias"
				, string.Empty));
		}
	}
}
