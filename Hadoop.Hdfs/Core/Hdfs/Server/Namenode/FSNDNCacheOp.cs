using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	internal class FSNDNCacheOp
	{
		/// <exception cref="System.IO.IOException"/>
		internal static CacheDirectiveInfo AddCacheDirective(FSNamesystem fsn, CacheManager
			 cacheManager, CacheDirectiveInfo directive, EnumSet<CacheFlag> flags, bool logRetryCache
			)
		{
			FSPermissionChecker pc = GetFsPermissionChecker(fsn);
			if (directive.GetId() != null)
			{
				throw new IOException("addDirective: you cannot specify an ID " + "for this operation."
					);
			}
			CacheDirectiveInfo effectiveDirective = cacheManager.AddDirective(directive, pc, 
				flags);
			fsn.GetEditLog().LogAddCacheDirectiveInfo(effectiveDirective, logRetryCache);
			return effectiveDirective;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void ModifyCacheDirective(FSNamesystem fsn, CacheManager cacheManager
			, CacheDirectiveInfo directive, EnumSet<CacheFlag> flags, bool logRetryCache)
		{
			FSPermissionChecker pc = GetFsPermissionChecker(fsn);
			cacheManager.ModifyDirective(directive, pc, flags);
			fsn.GetEditLog().LogModifyCacheDirectiveInfo(directive, logRetryCache);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void RemoveCacheDirective(FSNamesystem fsn, CacheManager cacheManager
			, long id, bool logRetryCache)
		{
			FSPermissionChecker pc = GetFsPermissionChecker(fsn);
			cacheManager.RemoveDirective(id, pc);
			fsn.GetEditLog().LogRemoveCacheDirectiveInfo(id, logRetryCache);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static BatchedRemoteIterator.BatchedListEntries<CacheDirectiveEntry> ListCacheDirectives
			(FSNamesystem fsn, CacheManager cacheManager, long startId, CacheDirectiveInfo filter
			)
		{
			FSPermissionChecker pc = GetFsPermissionChecker(fsn);
			return cacheManager.ListCacheDirectives(startId, filter, pc);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static CachePoolInfo AddCachePool(FSNamesystem fsn, CacheManager cacheManager
			, CachePoolInfo req, bool logRetryCache)
		{
			FSPermissionChecker pc = GetFsPermissionChecker(fsn);
			if (pc != null)
			{
				pc.CheckSuperuserPrivilege();
			}
			CachePoolInfo info = cacheManager.AddCachePool(req);
			fsn.GetEditLog().LogAddCachePool(info, logRetryCache);
			return info;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void ModifyCachePool(FSNamesystem fsn, CacheManager cacheManager, 
			CachePoolInfo req, bool logRetryCache)
		{
			FSPermissionChecker pc = GetFsPermissionChecker(fsn);
			if (pc != null)
			{
				pc.CheckSuperuserPrivilege();
			}
			cacheManager.ModifyCachePool(req);
			fsn.GetEditLog().LogModifyCachePool(req, logRetryCache);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void RemoveCachePool(FSNamesystem fsn, CacheManager cacheManager, 
			string cachePoolName, bool logRetryCache)
		{
			FSPermissionChecker pc = GetFsPermissionChecker(fsn);
			if (pc != null)
			{
				pc.CheckSuperuserPrivilege();
			}
			cacheManager.RemoveCachePool(cachePoolName);
			fsn.GetEditLog().LogRemoveCachePool(cachePoolName, logRetryCache);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static BatchedRemoteIterator.BatchedListEntries<CachePoolEntry> ListCachePools
			(FSNamesystem fsn, CacheManager cacheManager, string prevKey)
		{
			FSPermissionChecker pc = GetFsPermissionChecker(fsn);
			return cacheManager.ListCachePools(pc, prevKey);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		private static FSPermissionChecker GetFsPermissionChecker(FSNamesystem fsn)
		{
			return fsn.IsPermissionEnabled() ? fsn.GetPermissionChecker() : null;
		}
	}
}
