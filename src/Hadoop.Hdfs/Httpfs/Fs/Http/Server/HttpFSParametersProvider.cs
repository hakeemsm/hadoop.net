using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Http.Client;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Lib.Wsrs;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Server
{
	/// <summary>HttpFS ParametersProvider.</summary>
	public class HttpFSParametersProvider : ParametersProvider
	{
		private static readonly IDictionary<Enum, Type[]> ParamsDef = new Dictionary<Enum
			, Type[]>();

		static HttpFSParametersProvider()
		{
			ParamsDef[HttpFSFileSystem.Operation.Open] = new Type[] { typeof(HttpFSParametersProvider.OffsetParam
				), typeof(HttpFSParametersProvider.LenParam) };
			ParamsDef[HttpFSFileSystem.Operation.Getfilestatus] = new Type[] {  };
			ParamsDef[HttpFSFileSystem.Operation.Liststatus] = new Type[] { typeof(HttpFSParametersProvider.FilterParam
				) };
			ParamsDef[HttpFSFileSystem.Operation.Gethomedirectory] = new Type[] {  };
			ParamsDef[HttpFSFileSystem.Operation.Getcontentsummary] = new Type[] {  };
			ParamsDef[HttpFSFileSystem.Operation.Getfilechecksum] = new Type[] {  };
			ParamsDef[HttpFSFileSystem.Operation.Getfileblocklocations] = new Type[] {  };
			ParamsDef[HttpFSFileSystem.Operation.Getaclstatus] = new Type[] {  };
			ParamsDef[HttpFSFileSystem.Operation.Instrumentation] = new Type[] {  };
			ParamsDef[HttpFSFileSystem.Operation.Append] = new Type[] { typeof(HttpFSParametersProvider.DataParam
				) };
			ParamsDef[HttpFSFileSystem.Operation.Concat] = new Type[] { typeof(HttpFSParametersProvider.SourcesParam
				) };
			ParamsDef[HttpFSFileSystem.Operation.Truncate] = new Type[] { typeof(HttpFSParametersProvider.NewLengthParam
				) };
			ParamsDef[HttpFSFileSystem.Operation.Create] = new Type[] { typeof(HttpFSParametersProvider.PermissionParam
				), typeof(HttpFSParametersProvider.OverwriteParam), typeof(HttpFSParametersProvider.ReplicationParam
				), typeof(HttpFSParametersProvider.BlockSizeParam), typeof(HttpFSParametersProvider.DataParam
				) };
			ParamsDef[HttpFSFileSystem.Operation.Mkdirs] = new Type[] { typeof(HttpFSParametersProvider.PermissionParam
				) };
			ParamsDef[HttpFSFileSystem.Operation.Rename] = new Type[] { typeof(HttpFSParametersProvider.DestinationParam
				) };
			ParamsDef[HttpFSFileSystem.Operation.Setowner] = new Type[] { typeof(HttpFSParametersProvider.OwnerParam
				), typeof(HttpFSParametersProvider.GroupParam) };
			ParamsDef[HttpFSFileSystem.Operation.Setpermission] = new Type[] { typeof(HttpFSParametersProvider.PermissionParam
				) };
			ParamsDef[HttpFSFileSystem.Operation.Setreplication] = new Type[] { typeof(HttpFSParametersProvider.ReplicationParam
				) };
			ParamsDef[HttpFSFileSystem.Operation.Settimes] = new Type[] { typeof(HttpFSParametersProvider.ModifiedTimeParam
				), typeof(HttpFSParametersProvider.AccessTimeParam) };
			ParamsDef[HttpFSFileSystem.Operation.Delete] = new Type[] { typeof(HttpFSParametersProvider.RecursiveParam
				) };
			ParamsDef[HttpFSFileSystem.Operation.Setacl] = new Type[] { typeof(HttpFSParametersProvider.AclPermissionParam
				) };
			ParamsDef[HttpFSFileSystem.Operation.Removeacl] = new Type[] {  };
			ParamsDef[HttpFSFileSystem.Operation.Modifyaclentries] = new Type[] { typeof(HttpFSParametersProvider.AclPermissionParam
				) };
			ParamsDef[HttpFSFileSystem.Operation.Removeaclentries] = new Type[] { typeof(HttpFSParametersProvider.AclPermissionParam
				) };
			ParamsDef[HttpFSFileSystem.Operation.Removedefaultacl] = new Type[] {  };
			ParamsDef[HttpFSFileSystem.Operation.Setxattr] = new Type[] { typeof(HttpFSParametersProvider.XAttrNameParam
				), typeof(HttpFSParametersProvider.XAttrValueParam), typeof(HttpFSParametersProvider.XAttrSetFlagParam
				) };
			ParamsDef[HttpFSFileSystem.Operation.Removexattr] = new Type[] { typeof(HttpFSParametersProvider.XAttrNameParam
				) };
			ParamsDef[HttpFSFileSystem.Operation.Getxattrs] = new Type[] { typeof(HttpFSParametersProvider.XAttrNameParam
				), typeof(HttpFSParametersProvider.XAttrEncodingParam) };
			ParamsDef[HttpFSFileSystem.Operation.Listxattrs] = new Type[] {  };
		}

		public HttpFSParametersProvider()
			: base(HttpFSFileSystem.OpParam, typeof(HttpFSFileSystem.Operation), ParamsDef)
		{
		}

		/// <summary>Class for access-time parameter.</summary>
		public class AccessTimeParam : LongParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.AccessTimeParam;

			/// <summary>Constructor.</summary>
			public AccessTimeParam()
				: base(Name, -1l)
			{
			}
		}

		/// <summary>Class for block-size parameter.</summary>
		public class BlockSizeParam : LongParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.BlocksizeParam;

			/// <summary>Constructor.</summary>
			public BlockSizeParam()
				: base(Name, -1l)
			{
			}
		}

		/// <summary>Class for data parameter.</summary>
		public class DataParam : BooleanParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = "data";

			/// <summary>Constructor.</summary>
			public DataParam()
				: base(Name, false)
			{
			}
		}

		/// <summary>Class for operation parameter.</summary>
		public class OperationParam : EnumParam<HttpFSFileSystem.Operation>
		{
			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.OpParam;

			/// <summary>Constructor.</summary>
			public OperationParam(string operation)
				: base(Name, typeof(HttpFSFileSystem.Operation), HttpFSFileSystem.Operation.ValueOf
					(StringUtils.ToUpperCase(operation)))
			{
			}
		}

		/// <summary>Class for delete's recursive parameter.</summary>
		public class RecursiveParam : BooleanParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.RecursiveParam;

			/// <summary>Constructor.</summary>
			public RecursiveParam()
				: base(Name, false)
			{
			}
		}

		/// <summary>Class for filter parameter.</summary>
		public class FilterParam : StringParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = "filter";

			/// <summary>Constructor.</summary>
			public FilterParam()
				: base(Name, null)
			{
			}
		}

		/// <summary>Class for group parameter.</summary>
		public class GroupParam : StringParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.GroupParam;

			/// <summary>Constructor.</summary>
			public GroupParam()
				: base(Name, null)
			{
			}
		}

		/// <summary>Class for len parameter.</summary>
		public class LenParam : LongParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = "length";

			/// <summary>Constructor.</summary>
			public LenParam()
				: base(Name, -1l)
			{
			}
		}

		/// <summary>Class for modified-time parameter.</summary>
		public class ModifiedTimeParam : LongParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.ModificationTimeParam;

			/// <summary>Constructor.</summary>
			public ModifiedTimeParam()
				: base(Name, -1l)
			{
			}
		}

		/// <summary>Class for offset parameter.</summary>
		public class OffsetParam : LongParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = "offset";

			/// <summary>Constructor.</summary>
			public OffsetParam()
				: base(Name, 0l)
			{
			}
		}

		/// <summary>Class for newlength parameter.</summary>
		public class NewLengthParam : LongParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.NewLengthParam;

			/// <summary>Constructor.</summary>
			public NewLengthParam()
				: base(Name, 0l)
			{
			}
		}

		/// <summary>Class for overwrite parameter.</summary>
		public class OverwriteParam : BooleanParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.OverwriteParam;

			/// <summary>Constructor.</summary>
			public OverwriteParam()
				: base(Name, true)
			{
			}
		}

		/// <summary>Class for owner parameter.</summary>
		public class OwnerParam : StringParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.OwnerParam;

			/// <summary>Constructor.</summary>
			public OwnerParam()
				: base(Name, null)
			{
			}
		}

		/// <summary>Class for permission parameter.</summary>
		public class PermissionParam : ShortParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.PermissionParam;

			/// <summary>Constructor.</summary>
			public PermissionParam()
				: base(Name, HttpFSFileSystem.DefaultPermission, 8)
			{
			}
		}

		/// <summary>Class for AclPermission parameter.</summary>
		public class AclPermissionParam : StringParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.AclspecParam;

			/// <summary>Constructor.</summary>
			public AclPermissionParam()
				: base(Name, HttpFSFileSystem.AclspecDefault, Sharpen.Pattern.Compile(DFSConfigKeys
					.DfsWebhdfsAclPermissionPatternDefault))
			{
			}
		}

		/// <summary>Class for replication parameter.</summary>
		public class ReplicationParam : ShortParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.ReplicationParam;

			/// <summary>Constructor.</summary>
			public ReplicationParam()
				: base(Name, (short)-1)
			{
			}
		}

		/// <summary>Class for concat sources parameter.</summary>
		public class SourcesParam : StringParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.SourcesParam;

			/// <summary>Constructor.</summary>
			public SourcesParam()
				: base(Name, null)
			{
			}
		}

		/// <summary>Class for to-path parameter.</summary>
		public class DestinationParam : StringParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.DestinationParam;

			/// <summary>Constructor.</summary>
			public DestinationParam()
				: base(Name, null)
			{
			}
		}

		/// <summary>Class for xattr parameter.</summary>
		public class XAttrNameParam : StringParam
		{
			public const string XattrNameRegx = "^(user\\.|trusted\\.|system\\.|security\\.).+";

			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.XattrNameParam;

			private static readonly Sharpen.Pattern pattern = Sharpen.Pattern.Compile(XattrNameRegx
				);

			/// <summary>Constructor.</summary>
			public XAttrNameParam()
				: base(Name, null, pattern)
			{
			}
		}

		/// <summary>Class for xattr parameter.</summary>
		public class XAttrValueParam : StringParam
		{
			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.XattrValueParam;

			/// <summary>Constructor.</summary>
			public XAttrValueParam()
				: base(Name, null)
			{
			}
		}

		/// <summary>Class for xattr parameter.</summary>
		public class XAttrSetFlagParam : EnumSetParam<XAttrSetFlag>
		{
			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.XattrSetFlagParam;

			/// <summary>Constructor.</summary>
			public XAttrSetFlagParam()
				: base(Name, typeof(XAttrSetFlag), null)
			{
			}
		}

		/// <summary>Class for xattr parameter.</summary>
		public class XAttrEncodingParam : EnumParam<XAttrCodec>
		{
			/// <summary>Parameter name.</summary>
			public const string Name = HttpFSFileSystem.XattrEncodingParam;

			/// <summary>Constructor.</summary>
			public XAttrEncodingParam()
				: base(Name, typeof(XAttrCodec), null)
			{
			}
		}
	}
}
