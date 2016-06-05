using System;
using System.IO;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Mapreduce.Filecache;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	internal class JobResourceUploader
	{
		protected internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.JobResourceUploader
			));

		private FileSystem jtFs;

		internal JobResourceUploader(FileSystem submitFs)
		{
			this.jtFs = submitFs;
		}

		/// <summary>
		/// Upload and configure files, libjars, jobjars, and archives pertaining to
		/// the passed job.
		/// </summary>
		/// <param name="job">the job containing the files to be uploaded</param>
		/// <param name="submitJobDir">the submission directory of the job</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void UploadFiles(Job job, Path submitJobDir)
		{
			Configuration conf = job.GetConfiguration();
			short replication = (short)conf.GetInt(Job.SubmitReplication, Job.DefaultSubmitReplication
				);
			if (!(conf.GetBoolean(Job.UsedGenericParser, false)))
			{
				Log.Warn("Hadoop command-line option parsing not performed. " + "Implement the Tool interface and execute your application "
					 + "with ToolRunner to remedy this.");
			}
			// get all the command line arguments passed in by the user conf
			string files = conf.Get("tmpfiles");
			string libjars = conf.Get("tmpjars");
			string archives = conf.Get("tmparchives");
			string jobJar = job.GetJar();
			//
			// Figure out what fs the JobTracker is using. Copy the
			// job to it, under a temporary name. This allows DFS to work,
			// and under the local fs also provides UNIX-like object loading
			// semantics. (that is, if the job file is deleted right after
			// submission, we can still run the submission to completion)
			//
			// Create a number of filenames in the JobTracker's fs namespace
			Log.Debug("default FileSystem: " + jtFs.GetUri());
			if (jtFs.Exists(submitJobDir))
			{
				throw new IOException("Not submitting job. Job directory " + submitJobDir + " already exists!! This is unexpected.Please check what's there in"
					 + " that directory");
			}
			submitJobDir = jtFs.MakeQualified(submitJobDir);
			submitJobDir = new Path(submitJobDir.ToUri().GetPath());
			FsPermission mapredSysPerms = new FsPermission(JobSubmissionFiles.JobDirPermission
				);
			FileSystem.Mkdirs(jtFs, submitJobDir, mapredSysPerms);
			Path filesDir = JobSubmissionFiles.GetJobDistCacheFiles(submitJobDir);
			Path archivesDir = JobSubmissionFiles.GetJobDistCacheArchives(submitJobDir);
			Path libjarsDir = JobSubmissionFiles.GetJobDistCacheLibjars(submitJobDir);
			// add all the command line files/ jars and archive
			// first copy them to jobtrackers filesystem
			if (files != null)
			{
				FileSystem.Mkdirs(jtFs, filesDir, mapredSysPerms);
				string[] fileArr = files.Split(",");
				foreach (string tmpFile in fileArr)
				{
					URI tmpURI = null;
					try
					{
						tmpURI = new URI(tmpFile);
					}
					catch (URISyntaxException e)
					{
						throw new ArgumentException(e);
					}
					Path tmp = new Path(tmpURI);
					Path newPath = CopyRemoteFiles(filesDir, tmp, conf, replication);
					try
					{
						URI pathURI = GetPathURI(newPath, tmpURI.GetFragment());
						DistributedCache.AddCacheFile(pathURI, conf);
					}
					catch (URISyntaxException ue)
					{
						// should not throw a uri exception
						throw new IOException("Failed to create uri for " + tmpFile, ue);
					}
				}
			}
			if (libjars != null)
			{
				FileSystem.Mkdirs(jtFs, libjarsDir, mapredSysPerms);
				string[] libjarsArr = libjars.Split(",");
				foreach (string tmpjars in libjarsArr)
				{
					Path tmp = new Path(tmpjars);
					Path newPath = CopyRemoteFiles(libjarsDir, tmp, conf, replication);
					DistributedCache.AddFileToClassPath(new Path(newPath.ToUri().GetPath()), conf, jtFs
						);
				}
			}
			if (archives != null)
			{
				FileSystem.Mkdirs(jtFs, archivesDir, mapredSysPerms);
				string[] archivesArr = archives.Split(",");
				foreach (string tmpArchives in archivesArr)
				{
					URI tmpURI;
					try
					{
						tmpURI = new URI(tmpArchives);
					}
					catch (URISyntaxException e)
					{
						throw new ArgumentException(e);
					}
					Path tmp = new Path(tmpURI);
					Path newPath = CopyRemoteFiles(archivesDir, tmp, conf, replication);
					try
					{
						URI pathURI = GetPathURI(newPath, tmpURI.GetFragment());
						DistributedCache.AddCacheArchive(pathURI, conf);
					}
					catch (URISyntaxException ue)
					{
						// should not throw an uri excpetion
						throw new IOException("Failed to create uri for " + tmpArchives, ue);
					}
				}
			}
			if (jobJar != null)
			{
				// copy jar to JobTracker's fs
				// use jar name if job is not named.
				if (string.Empty.Equals(job.GetJobName()))
				{
					job.SetJobName(new Path(jobJar).GetName());
				}
				Path jobJarPath = new Path(jobJar);
				URI jobJarURI = jobJarPath.ToUri();
				// If the job jar is already in a global fs,
				// we don't need to copy it from local fs
				if (jobJarURI.GetScheme() == null || jobJarURI.GetScheme().Equals("file"))
				{
					CopyJar(jobJarPath, JobSubmissionFiles.GetJobJar(submitJobDir), replication);
					job.SetJar(JobSubmissionFiles.GetJobJar(submitJobDir).ToString());
				}
			}
			else
			{
				Log.Warn("No job jar file set.  User classes may not be found. " + "See Job or Job#setJar(String)."
					);
			}
			AddLog4jToDistributedCache(job, submitJobDir);
			// set the timestamps of the archives and files
			// set the public/private visibility of the archives and files
			ClientDistributedCacheManager.DetermineTimestampsAndCacheVisibilities(conf);
			// get DelegationToken for cached file
			ClientDistributedCacheManager.GetDelegationTokens(conf, job.GetCredentials());
		}

		// copies a file to the jobtracker filesystem and returns the path where it
		// was copied to
		/// <exception cref="System.IO.IOException"/>
		private Path CopyRemoteFiles(Path parentDir, Path originalPath, Configuration conf
			, short replication)
		{
			// check if we do not need to copy the files
			// is jt using the same file system.
			// just checking for uri strings... doing no dns lookups
			// to see if the filesystems are the same. This is not optimal.
			// but avoids name resolution.
			FileSystem remoteFs = null;
			remoteFs = originalPath.GetFileSystem(conf);
			if (CompareFs(remoteFs, jtFs))
			{
				return originalPath;
			}
			// this might have name collisions. copy will throw an exception
			// parse the original path to create new path
			Path newPath = new Path(parentDir, originalPath.GetName());
			FileUtil.Copy(remoteFs, originalPath, jtFs, newPath, false, conf);
			jtFs.SetReplication(newPath, replication);
			return newPath;
		}

		/*
		* see if two file systems are the same or not.
		*/
		private bool CompareFs(FileSystem srcFs, FileSystem destFs)
		{
			URI srcUri = srcFs.GetUri();
			URI dstUri = destFs.GetUri();
			if (srcUri.GetScheme() == null)
			{
				return false;
			}
			if (!srcUri.GetScheme().Equals(dstUri.GetScheme()))
			{
				return false;
			}
			string srcHost = srcUri.GetHost();
			string dstHost = dstUri.GetHost();
			if ((srcHost != null) && (dstHost != null))
			{
				try
				{
					srcHost = Sharpen.Extensions.GetAddressByName(srcHost).ToString();
					dstHost = Sharpen.Extensions.GetAddressByName(dstHost).ToString();
				}
				catch (UnknownHostException)
				{
					return false;
				}
				if (!srcHost.Equals(dstHost))
				{
					return false;
				}
			}
			else
			{
				if (srcHost == null && dstHost != null)
				{
					return false;
				}
				else
				{
					if (srcHost != null && dstHost == null)
					{
						return false;
					}
				}
			}
			// check for ports
			if (srcUri.GetPort() != dstUri.GetPort())
			{
				return false;
			}
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		private void CopyJar(Path originalJarPath, Path submitJarFile, short replication)
		{
			jtFs.CopyFromLocalFile(originalJarPath, submitJarFile);
			jtFs.SetReplication(submitJarFile, replication);
			jtFs.SetPermission(submitJarFile, new FsPermission(JobSubmissionFiles.JobFilePermission
				));
		}

		/// <exception cref="System.IO.IOException"/>
		private void AddLog4jToDistributedCache(Job job, Path jobSubmitDir)
		{
			Configuration conf = job.GetConfiguration();
			string log4jPropertyFile = conf.Get(MRJobConfig.MapreduceJobLog4jPropertiesFile, 
				string.Empty);
			if (!log4jPropertyFile.IsEmpty())
			{
				short replication = (short)conf.GetInt(Job.SubmitReplication, 10);
				CopyLog4jPropertyFile(job, jobSubmitDir, replication);
			}
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		private URI GetPathURI(Path destPath, string fragment)
		{
			URI pathURI = destPath.ToUri();
			if (pathURI.GetFragment() == null)
			{
				if (fragment == null)
				{
					pathURI = new URI(pathURI.ToString() + "#" + destPath.GetName());
				}
				else
				{
					pathURI = new URI(pathURI.ToString() + "#" + fragment);
				}
			}
			return pathURI;
		}

		// copy user specified log4j.property file in local
		// to HDFS with putting on distributed cache and adding its parent directory
		// to classpath.
		/// <exception cref="System.IO.IOException"/>
		private void CopyLog4jPropertyFile(Job job, Path submitJobDir, short replication)
		{
			Configuration conf = job.GetConfiguration();
			string file = ValidateFilePath(conf.Get(MRJobConfig.MapreduceJobLog4jPropertiesFile
				), conf);
			Log.Debug("default FileSystem: " + jtFs.GetUri());
			FsPermission mapredSysPerms = new FsPermission(JobSubmissionFiles.JobDirPermission
				);
			if (!jtFs.Exists(submitJobDir))
			{
				throw new IOException("Cannot find job submission directory! " + "It should just be created, so something wrong here."
					);
			}
			Path fileDir = JobSubmissionFiles.GetJobLog4jFile(submitJobDir);
			// first copy local log4j.properties file to HDFS under submitJobDir
			if (file != null)
			{
				FileSystem.Mkdirs(jtFs, fileDir, mapredSysPerms);
				URI tmpURI = null;
				try
				{
					tmpURI = new URI(file);
				}
				catch (URISyntaxException e)
				{
					throw new ArgumentException(e);
				}
				Path tmp = new Path(tmpURI);
				Path newPath = CopyRemoteFiles(fileDir, tmp, conf, replication);
				DistributedCache.AddFileToClassPath(new Path(newPath.ToUri().GetPath()), conf);
			}
		}

		/// <summary>takes input as a path string for file and verifies if it exist.</summary>
		/// <remarks>
		/// takes input as a path string for file and verifies if it exist. It defaults
		/// for file:/// if the files specified do not have a scheme. it returns the
		/// paths uri converted defaulting to file:///. So an input of /home/user/file1
		/// would return file:///home/user/file1
		/// </remarks>
		/// <param name="file"/>
		/// <param name="conf"/>
		/// <returns/>
		/// <exception cref="System.IO.IOException"/>
		private string ValidateFilePath(string file, Configuration conf)
		{
			if (file == null)
			{
				return null;
			}
			if (file.IsEmpty())
			{
				throw new ArgumentException("File name can't be empty string");
			}
			string finalPath;
			URI pathURI;
			try
			{
				pathURI = new URI(file);
			}
			catch (URISyntaxException e)
			{
				throw new ArgumentException(e);
			}
			Path path = new Path(pathURI);
			FileSystem localFs = FileSystem.GetLocal(conf);
			if (pathURI.GetScheme() == null)
			{
				// default to the local file system
				// check if the file exists or not first
				if (!localFs.Exists(path))
				{
					throw new FileNotFoundException("File " + file + " does not exist.");
				}
				finalPath = path.MakeQualified(localFs.GetUri(), localFs.GetWorkingDirectory()).ToString
					();
			}
			else
			{
				// check if the file exists in this file system
				// we need to recreate this filesystem object to copy
				// these files to the file system ResourceManager is running
				// on.
				FileSystem fs = path.GetFileSystem(conf);
				if (!fs.Exists(path))
				{
					throw new FileNotFoundException("File " + file + " does not exist.");
				}
				finalPath = path.MakeQualified(fs.GetUri(), fs.GetWorkingDirectory()).ToString();
			}
			return finalPath;
		}
	}
}
