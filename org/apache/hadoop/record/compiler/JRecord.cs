using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JRecord : org.apache.hadoop.record.compiler.JCompType
	{
		internal class JavaRecord : org.apache.hadoop.record.compiler.JCompType.JavaCompType
		{
			private string fullName;

			private string name;

			private string module;

			private System.Collections.Generic.List<org.apache.hadoop.record.compiler.JField<
				org.apache.hadoop.record.compiler.JType.JavaType>> fields = new System.Collections.Generic.List
				<org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.JavaType
				>>();

			internal JavaRecord(JRecord _enclosing, string name, System.Collections.Generic.List
				<org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType
				>> flist)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.fullName = name;
				int idx = name.LastIndexOf('.');
				this.name = Sharpen.Runtime.substring(name, idx + 1);
				this.module = Sharpen.Runtime.substring(name, 0, idx);
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType>> iter = flist.GetEnumerator(); iter.MoveNext
					(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType>
						 f = iter.Current;
					this.fields.add(new org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.JavaType
						>(f.getName(), f.getType().getJavaType()));
				}
			}

			internal override string getTypeIDObjectString()
			{
				return "new org.apache.hadoop.record.meta.StructTypeID(" + this.fullName + ".getTypeInfo())";
			}

			internal override void genSetRTIFilter(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, System.Collections.Generic.IDictionary<string, int> nestedStructMap)
			{
				// ignore, if we'ev already set the type filter for this record
				if (!nestedStructMap.Contains(this.fullName))
				{
					// we set the RTI filter here
					cb.append(this.fullName + ".setTypeFilter(rti.getNestedStructTypeInfo(\"" + this.
						name + "\"));\n");
					nestedStructMap[this.fullName] = null;
				}
			}

			// for each typeInfo in the filter, we see if there's a similar one in the record. 
			// Since we store typeInfos in ArrayLists, thsi search is O(n squared). We do it faster
			// if we also store a map (of TypeInfo to index), but since setupRtiFields() is called
			// only once when deserializing, we're sticking with the former, as the code is easier.  
			internal virtual void genSetupRtiFields(org.apache.hadoop.record.compiler.CodeBuffer
				 cb)
			{
				cb.append("private static void setupRtiFields()\n{\n");
				cb.append("if (null == " + org.apache.hadoop.record.compiler.Consts.RTI_FILTER + 
					") return;\n");
				cb.append("// we may already have done this\n");
				cb.append("if (null != " + org.apache.hadoop.record.compiler.Consts.RTI_FILTER_FIELDS
					 + ") return;\n");
				cb.append("int " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "i, " + 
					org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "j;\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RTI_FILTER_FIELDS + " = new int ["
					 + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "rtiFilter.getFieldTypeInfos().size()];\n"
					);
				cb.append("for (" + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "i=0; "
					 + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "i<" + org.apache.hadoop.record.compiler.Consts
					.RTI_FILTER_FIELDS + ".length; " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX
					 + "i++) {\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RTI_FILTER_FIELDS + "[" + org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "i] = 0;\n");
				cb.append("}\n");
				cb.append("java.util.Iterator<org.apache.hadoop.record.meta." + "FieldTypeInfo> "
					 + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "itFilter = " + org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "rtiFilter.getFieldTypeInfos().iterator();\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "i=0;\n");
				cb.append("while (" + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "itFilter.hasNext()) {\n"
					);
				cb.append("org.apache.hadoop.record.meta.FieldTypeInfo " + org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "tInfoFilter = " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX
					 + "itFilter.next();\n");
				cb.append("java.util.Iterator<org.apache.hadoop.record.meta." + "FieldTypeInfo> "
					 + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "it = " + org.apache.hadoop.record.compiler.Consts
					.RTI_VAR + ".getFieldTypeInfos().iterator();\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "j=1;\n");
				cb.append("while (" + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "it.hasNext()) {\n"
					);
				cb.append("org.apache.hadoop.record.meta.FieldTypeInfo " + org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "tInfo = " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX +
					 "it.next();\n");
				cb.append("if (" + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "tInfo.equals("
					 + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "tInfoFilter)) {\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RTI_FILTER_FIELDS + "[" + org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "i] = " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "j;\n"
					);
				cb.append("break;\n");
				cb.append("}\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "j++;\n");
				cb.append("}\n");
				/*int ct = 0;
				for (Iterator<JField<JavaType>> i = fields.iterator(); i.hasNext();) {
				ct++;
				JField<JavaType> jf = i.next();
				JavaType type = jf.getType();
				String name = jf.getName();
				if (ct != 1) {
				cb.append("else ");
				}
				type.genRtiFieldCondition(cb, name, ct);
				}
				if (ct != 0) {
				cb.append("else {\n");
				cb.append("rtiFilterFields[i] = 0;\n");
				cb.append("}\n");
				}*/
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "i++;\n");
				cb.append("}\n");
				cb.append("}\n");
			}

			internal override void genReadMethod(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string fname, string tag, bool decl)
			{
				if (decl)
				{
					cb.append(this.fullName + " " + fname + ";\n");
				}
				cb.append(fname + "= new " + this.fullName + "();\n");
				cb.append(fname + ".deserialize(" + org.apache.hadoop.record.compiler.Consts.RECORD_INPUT
					 + ",\"" + tag + "\");\n");
			}

			internal override void genWriteMethod(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string fname, string tag)
			{
				cb.append(fname + ".serialize(" + org.apache.hadoop.record.compiler.Consts.RECORD_OUTPUT
					 + ",\"" + tag + "\");\n");
			}

			internal override void genSlurpBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string b, string s, string l)
			{
				cb.append("{\n");
				cb.append("int r = " + this.fullName + ".Comparator.slurpRaw(" + b + "," + s + ","
					 + l + ");\n");
				cb.append(s + "+=r; " + l + "-=r;\n");
				cb.append("}\n");
			}

			internal override void genCompareBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb)
			{
				cb.append("{\n");
				cb.append("int r1 = " + this.fullName + ".Comparator.compareRaw(b1,s1,l1,b2,s2,l2);\n"
					);
				cb.append("if (r1 <= 0) { return r1; }\n");
				cb.append("s1+=r1; s2+=r1; l1-=r1; l2-=r1;\n");
				cb.append("}\n");
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void genCode(string destDir, System.Collections.Generic.List<string
				> options)
			{
				string pkg = this.module;
				string pkgpath = pkg.replaceAll("\\.", "/");
				java.io.File pkgdir = new java.io.File(destDir, pkgpath);
				java.io.File jfile = new java.io.File(pkgdir, this.name + ".java");
				if (!pkgdir.exists())
				{
					// create the pkg directory
					bool ret = pkgdir.mkdirs();
					if (!ret)
					{
						throw new System.IO.IOException("Cannnot create directory: " + pkgpath);
					}
				}
				else
				{
					if (!pkgdir.isDirectory())
					{
						// not a directory
						throw new System.IO.IOException(pkgpath + " is not a directory.");
					}
				}
				org.apache.hadoop.record.compiler.CodeBuffer cb = new org.apache.hadoop.record.compiler.CodeBuffer
					();
				cb.append("// File generated by hadoop record compiler. Do not edit.\n");
				cb.append("package " + this.module + ";\n\n");
				cb.append("public class " + this.name + " extends org.apache.hadoop.record.Record {\n"
					);
				// type information declarations
				cb.append("private static final " + "org.apache.hadoop.record.meta.RecordTypeInfo "
					 + org.apache.hadoop.record.compiler.Consts.RTI_VAR + ";\n");
				cb.append("private static " + "org.apache.hadoop.record.meta.RecordTypeInfo " + org.apache.hadoop.record.compiler.Consts
					.RTI_FILTER + ";\n");
				cb.append("private static int[] " + org.apache.hadoop.record.compiler.Consts.RTI_FILTER_FIELDS
					 + ";\n");
				// static init for type information
				cb.append("static {\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RTI_VAR + " = " + "new org.apache.hadoop.record.meta.RecordTypeInfo(\""
					 + this.name + "\");\n");
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.JavaType>> i = this.fields.GetEnumerator
					(); i.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.JavaType
						> jf = i.Current;
					string name = jf.getName();
					org.apache.hadoop.record.compiler.JType.JavaType type = jf.getType();
					type.genStaticTypeInfo(cb, name);
				}
				cb.append("}\n\n");
				// field definitions
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.JavaType>> i_1 = this.fields.GetEnumerator
					(); i_1.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.JavaType
						> jf = i_1.Current;
					string name = jf.getName();
					org.apache.hadoop.record.compiler.JType.JavaType type = jf.getType();
					type.genDecl(cb, name);
				}
				// default constructor
				cb.append("public " + this.name + "() { }\n");
				// constructor
				cb.append("public " + this.name + "(\n");
				int fIdx = 0;
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.JavaType>> i_2 = this.fields.GetEnumerator
					(); i_2.MoveNext(); fIdx++)
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.JavaType
						> jf = i_2.Current;
					string name = jf.getName();
					org.apache.hadoop.record.compiler.JType.JavaType type = jf.getType();
					type.genConstructorParam(cb, name);
					cb.append((!i_2.MoveNext()) ? string.Empty : ",\n");
				}
				cb.append(") {\n");
				fIdx = 0;
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.JavaType>> i_3 = this.fields.GetEnumerator
					(); i_3.MoveNext(); fIdx++)
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.JavaType
						> jf = i_3.Current;
					string name = jf.getName();
					org.apache.hadoop.record.compiler.JType.JavaType type = jf.getType();
					type.genConstructorSet(cb, name);
				}
				cb.append("}\n");
				// getter/setter for type info
				cb.append("public static org.apache.hadoop.record.meta.RecordTypeInfo" + " getTypeInfo() {\n"
					);
				cb.append("return " + org.apache.hadoop.record.compiler.Consts.RTI_VAR + ";\n");
				cb.append("}\n");
				cb.append("public static void setTypeFilter(" + "org.apache.hadoop.record.meta.RecordTypeInfo rti) {\n"
					);
				cb.append("if (null == rti) return;\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RTI_FILTER + " = rti;\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RTI_FILTER_FIELDS + " = null;\n"
					);
				// set RTIFilter for nested structs.
				// To prevent setting up the type filter for the same struct more than once, 
				// we use a hash map to keep track of what we've set. 
				System.Collections.Generic.IDictionary<string, int> nestedStructMap = new System.Collections.Generic.Dictionary
					<string, int>();
				foreach (org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.JavaType
					> jf_1 in this.fields)
				{
					org.apache.hadoop.record.compiler.JType.JavaType type = jf_1.getType();
					type.genSetRTIFilter(cb, nestedStructMap);
				}
				cb.append("}\n");
				// setupRtiFields()
				this.genSetupRtiFields(cb);
				// getters/setters for member variables
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.JavaType>> i_4 = this.fields.GetEnumerator
					(); i_4.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.JavaType
						> jf = i_4.Current;
					string name = jf_1.getName();
					org.apache.hadoop.record.compiler.JType.JavaType type = jf_1.getType();
					type.genGetSet(cb, name);
				}
				// serialize()
				cb.append("public void serialize(" + "final org.apache.hadoop.record.RecordOutput "
					 + org.apache.hadoop.record.compiler.Consts.RECORD_OUTPUT + ", final String " + 
					org.apache.hadoop.record.compiler.Consts.TAG + ")\n" + "throws java.io.IOException {\n"
					);
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_OUTPUT + ".startRecord(this,"
					 + org.apache.hadoop.record.compiler.Consts.TAG + ");\n");
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.JavaType>> i_5 = this.fields.GetEnumerator
					(); i_5.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.JavaType
						> jf = i_5.Current;
					string name = jf_1.getName();
					org.apache.hadoop.record.compiler.JType.JavaType type = jf_1.getType();
					type.genWriteMethod(cb, name, name);
				}
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_OUTPUT + ".endRecord(this,"
					 + org.apache.hadoop.record.compiler.Consts.TAG + ");\n");
				cb.append("}\n");
				// deserializeWithoutFilter()
				cb.append("private void deserializeWithoutFilter(" + "final org.apache.hadoop.record.RecordInput "
					 + org.apache.hadoop.record.compiler.Consts.RECORD_INPUT + ", final String " + org.apache.hadoop.record.compiler.Consts
					.TAG + ")\n" + "throws java.io.IOException {\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_INPUT + ".startRecord("
					 + org.apache.hadoop.record.compiler.Consts.TAG + ");\n");
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.JavaType>> i_6 = this.fields.GetEnumerator
					(); i_6.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.JavaType
						> jf = i_6.Current;
					string name = jf_1.getName();
					org.apache.hadoop.record.compiler.JType.JavaType type = jf_1.getType();
					type.genReadMethod(cb, name, name, false);
				}
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_INPUT + ".endRecord(" +
					 org.apache.hadoop.record.compiler.Consts.TAG + ");\n");
				cb.append("}\n");
				// deserialize()
				cb.append("public void deserialize(final " + "org.apache.hadoop.record.RecordInput "
					 + org.apache.hadoop.record.compiler.Consts.RECORD_INPUT + ", final String " + org.apache.hadoop.record.compiler.Consts
					.TAG + ")\n" + "throws java.io.IOException {\n");
				cb.append("if (null == " + org.apache.hadoop.record.compiler.Consts.RTI_FILTER + 
					") {\n");
				cb.append("deserializeWithoutFilter(" + org.apache.hadoop.record.compiler.Consts.
					RECORD_INPUT + ", " + org.apache.hadoop.record.compiler.Consts.TAG + ");\n");
				cb.append("return;\n");
				cb.append("}\n");
				cb.append("// if we're here, we need to read based on version info\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_INPUT + ".startRecord("
					 + org.apache.hadoop.record.compiler.Consts.TAG + ");\n");
				cb.append("setupRtiFields();\n");
				cb.append("for (int " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "i=0; "
					 + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "i<" + org.apache.hadoop.record.compiler.Consts
					.RTI_FILTER + ".getFieldTypeInfos().size(); " + org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "i++) {\n");
				int ct = 0;
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.JavaType>> i_7 = this.fields.GetEnumerator
					(); i_7.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.JavaType
						> jf = i_7.Current;
					string name = jf_1.getName();
					org.apache.hadoop.record.compiler.JType.JavaType type = jf_1.getType();
					ct++;
					if (1 != ct)
					{
						cb.append("else ");
					}
					cb.append("if (" + ct + " == " + org.apache.hadoop.record.compiler.Consts.RTI_FILTER_FIELDS
						 + "[" + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "i]) {\n");
					type.genReadMethod(cb, name, name, false);
					cb.append("}\n");
				}
				if (0 != ct)
				{
					cb.append("else {\n");
					cb.append("java.util.ArrayList<" + "org.apache.hadoop.record.meta.FieldTypeInfo> typeInfos = "
						 + "(java.util.ArrayList<" + "org.apache.hadoop.record.meta.FieldTypeInfo>)" + "("
						 + org.apache.hadoop.record.compiler.Consts.RTI_FILTER + ".getFieldTypeInfos());\n"
						);
					cb.append("org.apache.hadoop.record.meta.Utils.skip(" + org.apache.hadoop.record.compiler.Consts
						.RECORD_INPUT + ", " + "typeInfos.get(" + org.apache.hadoop.record.compiler.Consts
						.RIO_PREFIX + "i).getFieldID(), typeInfos.get(" + org.apache.hadoop.record.compiler.Consts
						.RIO_PREFIX + "i).getTypeID());\n");
					cb.append("}\n");
				}
				cb.append("}\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_INPUT + ".endRecord(" +
					 org.apache.hadoop.record.compiler.Consts.TAG + ");\n");
				cb.append("}\n");
				// compareTo()
				cb.append("public int compareTo (final Object " + org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "peer_) throws ClassCastException {\n");
				cb.append("if (!(" + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "peer_ instanceof "
					 + this.name + ")) {\n");
				cb.append("throw new ClassCastException(\"Comparing different types of records.\");\n"
					);
				cb.append("}\n");
				cb.append(this.name + " " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX +
					 "peer = (" + this.name + ") " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX
					 + "peer_;\n");
				cb.append("int " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret = 0;\n"
					);
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.JavaType>> i_8 = this.fields.GetEnumerator
					(); i_8.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.JavaType
						> jf = i_8.Current;
					string name = jf_1.getName();
					org.apache.hadoop.record.compiler.JType.JavaType type = jf_1.getType();
					type.genCompareTo(cb, name, org.apache.hadoop.record.compiler.Consts.RIO_PREFIX +
						 "peer." + name);
					cb.append("if (" + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret != 0) return "
						 + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret;\n");
				}
				cb.append("return " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret;\n"
					);
				cb.append("}\n");
				// equals()
				cb.append("public boolean equals(final Object " + org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "peer_) {\n");
				cb.append("if (!(" + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "peer_ instanceof "
					 + this.name + ")) {\n");
				cb.append("return false;\n");
				cb.append("}\n");
				cb.append("if (" + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "peer_ == this) {\n"
					);
				cb.append("return true;\n");
				cb.append("}\n");
				cb.append(this.name + " " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX +
					 "peer = (" + this.name + ") " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX
					 + "peer_;\n");
				cb.append("boolean " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret = false;\n"
					);
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.JavaType>> i_9 = this.fields.GetEnumerator
					(); i_9.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.JavaType
						> jf = i_9.Current;
					string name = jf_1.getName();
					org.apache.hadoop.record.compiler.JType.JavaType type = jf_1.getType();
					type.genEquals(cb, name, org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "peer."
						 + name);
					cb.append("if (!" + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret) return "
						 + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret;\n");
				}
				cb.append("return " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret;\n"
					);
				cb.append("}\n");
				// clone()
				cb.append("public Object clone() throws CloneNotSupportedException {\n");
				cb.append(this.name + " " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX +
					 "other = new " + this.name + "();\n");
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.JavaType>> i_10 = this.fields.GetEnumerator
					(); i_10.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.JavaType
						> jf = i_10.Current;
					string name = jf_1.getName();
					org.apache.hadoop.record.compiler.JType.JavaType type = jf_1.getType();
					type.genClone(cb, name);
				}
				cb.append("return " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "other;\n"
					);
				cb.append("}\n");
				cb.append("public int hashCode() {\n");
				cb.append("int " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "result = 17;\n"
					);
				cb.append("int " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret;\n"
					);
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.JavaType>> i_11 = this.fields.GetEnumerator
					(); i_11.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.JavaType
						> jf = i_11.Current;
					string name = jf_1.getName();
					org.apache.hadoop.record.compiler.JType.JavaType type = jf_1.getType();
					type.genHashCode(cb, name);
					cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "result = 37*" + 
						org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "result + " + org.apache.hadoop.record.compiler.Consts
						.RIO_PREFIX + "ret;\n");
				}
				cb.append("return " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "result;\n"
					);
				cb.append("}\n");
				cb.append("public static String signature() {\n");
				cb.append("return \"" + this._enclosing.getSignature() + "\";\n");
				cb.append("}\n");
				cb.append("public static class Comparator extends" + " org.apache.hadoop.record.RecordComparator {\n"
					);
				cb.append("public Comparator() {\n");
				cb.append("super(" + this.name + ".class);\n");
				cb.append("}\n");
				cb.append("static public int slurpRaw(byte[] b, int s, int l) {\n");
				cb.append("try {\n");
				cb.append("int os = s;\n");
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.JavaType>> i_12 = this.fields.GetEnumerator
					(); i_12.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.JavaType
						> jf = i_12.Current;
					string name = jf_1.getName();
					org.apache.hadoop.record.compiler.JType.JavaType type = jf_1.getType();
					type.genSlurpBytes(cb, "b", "s", "l");
				}
				cb.append("return (os - s);\n");
				cb.append("} catch(java.io.IOException e) {\n");
				cb.append("throw new RuntimeException(e);\n");
				cb.append("}\n");
				cb.append("}\n");
				cb.append("static public int compareRaw(byte[] b1, int s1, int l1,\n");
				cb.append("                             byte[] b2, int s2, int l2) {\n");
				cb.append("try {\n");
				cb.append("int os1 = s1;\n");
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.JavaType>> i_13 = this.fields.GetEnumerator
					(); i_13.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.JavaType
						> jf = i_13.Current;
					string name = jf_1.getName();
					org.apache.hadoop.record.compiler.JType.JavaType type = jf_1.getType();
					type.genCompareBytes(cb);
				}
				cb.append("return (os1 - s1);\n");
				cb.append("} catch(java.io.IOException e) {\n");
				cb.append("throw new RuntimeException(e);\n");
				cb.append("}\n");
				cb.append("}\n");
				cb.append("public int compare(byte[] b1, int s1, int l1,\n");
				cb.append("                   byte[] b2, int s2, int l2) {\n");
				cb.append("int ret = compareRaw(b1,s1,l1,b2,s2,l2);\n");
				cb.append("return (ret == -1)? -1 : ((ret==0)? 1 : 0);");
				cb.append("}\n");
				cb.append("}\n\n");
				cb.append("static {\n");
				cb.append("org.apache.hadoop.record.RecordComparator.define(" + this.name + ".class, new Comparator());\n"
					);
				cb.append("}\n");
				cb.append("}\n");
				java.io.FileWriter jj = new java.io.FileWriter(jfile);
				try
				{
					jj.write(cb.ToString());
				}
				finally
				{
					jj.close();
				}
			}

			private readonly JRecord _enclosing;
		}

		internal class CppRecord : org.apache.hadoop.record.compiler.JCompType.CppCompType
		{
			private string fullName;

			private string name;

			private string module;

			private System.Collections.Generic.List<org.apache.hadoop.record.compiler.JField<
				org.apache.hadoop.record.compiler.JType.CppType>> fields = new System.Collections.Generic.List
				<org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.CppType
				>>();

			internal CppRecord(JRecord _enclosing, string name, System.Collections.Generic.List
				<org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType
				>> flist)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.fullName = name.replaceAll("\\.", "::");
				int idx = name.LastIndexOf('.');
				this.name = Sharpen.Runtime.substring(name, idx + 1);
				this.module = Sharpen.Runtime.substring(name, 0, idx).replaceAll("\\.", "::");
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType>> iter = flist.GetEnumerator(); iter.MoveNext
					(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType>
						 f = iter.Current;
					this.fields.add(new org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.CppType
						>(f.getName(), f.getType().getCppType()));
				}
			}

			internal override string getTypeIDObjectString()
			{
				return "new ::hadoop::StructTypeID(" + this.fullName + "::getTypeInfo().getFieldTypeInfos())";
			}

			internal virtual string genDecl(string fname)
			{
				return "  " + this.name + " " + fname + ";\n";
			}

			internal override void genSetRTIFilter(org.apache.hadoop.record.compiler.CodeBuffer
				 cb)
			{
				// we set the RTI filter here
				cb.append(this.fullName + "::setTypeFilter(rti.getNestedStructTypeInfo(\"" + this
					.name + "\"));\n");
			}

			internal virtual void genSetupRTIFields(org.apache.hadoop.record.compiler.CodeBuffer
				 cb)
			{
				cb.append("void " + this.fullName + "::setupRtiFields() {\n");
				cb.append("if (NULL == p" + org.apache.hadoop.record.compiler.Consts.RTI_FILTER +
					 ") return;\n");
				cb.append("if (NULL != p" + org.apache.hadoop.record.compiler.Consts.RTI_FILTER_FIELDS
					 + ") return;\n");
				cb.append("p" + org.apache.hadoop.record.compiler.Consts.RTI_FILTER_FIELDS + " = new int[p"
					 + org.apache.hadoop.record.compiler.Consts.RTI_FILTER + "->getFieldTypeInfos().size()];\n"
					);
				cb.append("for (unsigned int " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX
					 + "i=0; " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "i<p" + org.apache.hadoop.record.compiler.Consts
					.RTI_FILTER + "->getFieldTypeInfos().size(); " + org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "i++) {\n");
				cb.append("p" + org.apache.hadoop.record.compiler.Consts.RTI_FILTER_FIELDS + "[" 
					+ org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "i] = 0;\n");
				cb.append("}\n");
				cb.append("for (unsigned int " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX
					 + "i=0; " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "i<p" + org.apache.hadoop.record.compiler.Consts
					.RTI_FILTER + "->getFieldTypeInfos().size(); " + org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "i++) {\n");
				cb.append("for (unsigned int " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX
					 + "j=0; " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "j<p" + org.apache.hadoop.record.compiler.Consts
					.RTI_VAR + "->getFieldTypeInfos().size(); " + org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "j++) {\n");
				cb.append("if (*(p" + org.apache.hadoop.record.compiler.Consts.RTI_FILTER + "->getFieldTypeInfos()["
					 + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "i]) == *(p" + org.apache.hadoop.record.compiler.Consts
					.RTI_VAR + "->getFieldTypeInfos()[" + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX
					 + "j])) {\n");
				cb.append("p" + org.apache.hadoop.record.compiler.Consts.RTI_FILTER_FIELDS + "[" 
					+ org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "i] = " + org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "j+1;\n");
				cb.append("break;\n");
				cb.append("}\n");
				cb.append("}\n");
				cb.append("}\n");
				cb.append("}\n");
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void genCode(java.io.FileWriter hh, java.io.FileWriter cc, System.Collections.Generic.List
				<string> options)
			{
				org.apache.hadoop.record.compiler.CodeBuffer hb = new org.apache.hadoop.record.compiler.CodeBuffer
					();
				string[] ns = this.module.split("::");
				for (int i = 0; i < ns.Length; i++)
				{
					hb.append("namespace " + ns[i] + " {\n");
				}
				hb.append("class " + this.name + " : public ::hadoop::Record {\n");
				hb.append("private:\n");
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.CppType>> i_1 = this.fields.GetEnumerator
					(); i_1.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.CppType
						> jf = i_1.Current;
					string name = jf.getName();
					org.apache.hadoop.record.compiler.JType.CppType type = jf.getType();
					type.genDecl(hb, name);
				}
				// type info vars
				hb.append("static ::hadoop::RecordTypeInfo* p" + org.apache.hadoop.record.compiler.Consts
					.RTI_VAR + ";\n");
				hb.append("static ::hadoop::RecordTypeInfo* p" + org.apache.hadoop.record.compiler.Consts
					.RTI_FILTER + ";\n");
				hb.append("static int* p" + org.apache.hadoop.record.compiler.Consts.RTI_FILTER_FIELDS
					 + ";\n");
				hb.append("static ::hadoop::RecordTypeInfo* setupTypeInfo();\n");
				hb.append("static void setupRtiFields();\n");
				hb.append("virtual void deserializeWithoutFilter(::hadoop::IArchive& " + org.apache.hadoop.record.compiler.Consts
					.RECORD_INPUT + ", const char* " + org.apache.hadoop.record.compiler.Consts.TAG 
					+ ");\n");
				hb.append("public:\n");
				hb.append("static const ::hadoop::RecordTypeInfo& getTypeInfo() " + "{return *p" 
					+ org.apache.hadoop.record.compiler.Consts.RTI_VAR + ";}\n");
				hb.append("static void setTypeFilter(const ::hadoop::RecordTypeInfo& rti);\n");
				hb.append("static void setTypeFilter(const ::hadoop::RecordTypeInfo* prti);\n");
				hb.append("virtual void serialize(::hadoop::OArchive& " + org.apache.hadoop.record.compiler.Consts
					.RECORD_OUTPUT + ", const char* " + org.apache.hadoop.record.compiler.Consts.TAG
					 + ") const;\n");
				hb.append("virtual void deserialize(::hadoop::IArchive& " + org.apache.hadoop.record.compiler.Consts
					.RECORD_INPUT + ", const char* " + org.apache.hadoop.record.compiler.Consts.TAG 
					+ ");\n");
				hb.append("virtual const ::std::string& type() const;\n");
				hb.append("virtual const ::std::string& signature() const;\n");
				hb.append("virtual bool operator<(const " + this.name + "& peer_) const;\n");
				hb.append("virtual bool operator==(const " + this.name + "& peer_) const;\n");
				hb.append("virtual ~" + this.name + "() {};\n");
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.CppType>> i_2 = this.fields.GetEnumerator
					(); i_2.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.CppType
						> jf = i_2.Current;
					string name = jf.getName();
					org.apache.hadoop.record.compiler.JType.CppType type = jf.getType();
					type.genGetSet(hb, name);
				}
				hb.append("}; // end record " + this.name + "\n");
				for (int i_3 = ns.Length - 1; i_3 >= 0; i_3--)
				{
					hb.append("} // end namespace " + ns[i_3] + "\n");
				}
				hh.write(hb.ToString());
				org.apache.hadoop.record.compiler.CodeBuffer cb = new org.apache.hadoop.record.compiler.CodeBuffer
					();
				// initialize type info vars
				cb.append("::hadoop::RecordTypeInfo* " + this.fullName + "::p" + org.apache.hadoop.record.compiler.Consts
					.RTI_VAR + " = " + this.fullName + "::setupTypeInfo();\n");
				cb.append("::hadoop::RecordTypeInfo* " + this.fullName + "::p" + org.apache.hadoop.record.compiler.Consts
					.RTI_FILTER + " = NULL;\n");
				cb.append("int* " + this.fullName + "::p" + org.apache.hadoop.record.compiler.Consts
					.RTI_FILTER_FIELDS + " = NULL;\n\n");
				// setupTypeInfo()
				cb.append("::hadoop::RecordTypeInfo* " + this.fullName + "::setupTypeInfo() {\n");
				cb.append("::hadoop::RecordTypeInfo* p = new ::hadoop::RecordTypeInfo(\"" + this.
					name + "\");\n");
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.CppType>> i_4 = this.fields.GetEnumerator
					(); i_4.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.CppType
						> jf = i_4.Current;
					string name = jf.getName();
					org.apache.hadoop.record.compiler.JType.CppType type = jf.getType();
					type.genStaticTypeInfo(cb, name);
				}
				cb.append("return p;\n");
				cb.append("}\n");
				// setTypeFilter()
				cb.append("void " + this.fullName + "::setTypeFilter(const " + "::hadoop::RecordTypeInfo& rti) {\n"
					);
				cb.append("if (NULL != p" + org.apache.hadoop.record.compiler.Consts.RTI_FILTER +
					 ") {\n");
				cb.append("delete p" + org.apache.hadoop.record.compiler.Consts.RTI_FILTER + ";\n"
					);
				cb.append("}\n");
				cb.append("p" + org.apache.hadoop.record.compiler.Consts.RTI_FILTER + " = new ::hadoop::RecordTypeInfo(rti);\n"
					);
				cb.append("if (NULL != p" + org.apache.hadoop.record.compiler.Consts.RTI_FILTER_FIELDS
					 + ") {\n");
				cb.append("delete p" + org.apache.hadoop.record.compiler.Consts.RTI_FILTER_FIELDS
					 + ";\n");
				cb.append("}\n");
				cb.append("p" + org.apache.hadoop.record.compiler.Consts.RTI_FILTER_FIELDS + " = NULL;\n"
					);
				// set RTIFilter for nested structs. We may end up with multiple lines that 
				// do the same thing, if the same struct is nested in more than one field, 
				// but that's OK. 
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.CppType>> i_5 = this.fields.GetEnumerator
					(); i_5.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.CppType
						> jf = i_5.Current;
					org.apache.hadoop.record.compiler.JType.CppType type = jf.getType();
					type.genSetRTIFilter(cb);
				}
				cb.append("}\n");
				// setTypeFilter()
				cb.append("void " + this.fullName + "::setTypeFilter(const " + "::hadoop::RecordTypeInfo* prti) {\n"
					);
				cb.append("if (NULL != prti) {\n");
				cb.append("setTypeFilter(*prti);\n");
				cb.append("}\n");
				cb.append("}\n");
				// setupRtiFields()
				this.genSetupRTIFields(cb);
				// serialize()
				cb.append("void " + this.fullName + "::serialize(::hadoop::OArchive& " + org.apache.hadoop.record.compiler.Consts
					.RECORD_OUTPUT + ", const char* " + org.apache.hadoop.record.compiler.Consts.TAG
					 + ") const {\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_OUTPUT + ".startRecord(*this,"
					 + org.apache.hadoop.record.compiler.Consts.TAG + ");\n");
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.CppType>> i_6 = this.fields.GetEnumerator
					(); i_6.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.CppType
						> jf = i_6.Current;
					string name = jf.getName();
					org.apache.hadoop.record.compiler.JType.CppType type = jf.getType();
					if (type is org.apache.hadoop.record.compiler.JBuffer.CppBuffer)
					{
						cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_OUTPUT + ".serialize(" 
							+ name + "," + name + ".length(),\"" + name + "\");\n");
					}
					else
					{
						cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_OUTPUT + ".serialize(" 
							+ name + ",\"" + name + "\");\n");
					}
				}
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_OUTPUT + ".endRecord(*this,"
					 + org.apache.hadoop.record.compiler.Consts.TAG + ");\n");
				cb.append("return;\n");
				cb.append("}\n");
				// deserializeWithoutFilter()
				cb.append("void " + this.fullName + "::deserializeWithoutFilter(::hadoop::IArchive& "
					 + org.apache.hadoop.record.compiler.Consts.RECORD_INPUT + ", const char* " + org.apache.hadoop.record.compiler.Consts
					.TAG + ") {\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_INPUT + ".startRecord(*this,"
					 + org.apache.hadoop.record.compiler.Consts.TAG + ");\n");
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.CppType>> i_7 = this.fields.GetEnumerator
					(); i_7.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.CppType
						> jf = i_7.Current;
					string name = jf.getName();
					org.apache.hadoop.record.compiler.JType.CppType type = jf.getType();
					if (type is org.apache.hadoop.record.compiler.JBuffer.CppBuffer)
					{
						cb.append("{\nsize_t len=0; " + org.apache.hadoop.record.compiler.Consts.RECORD_INPUT
							 + ".deserialize(" + name + ",len,\"" + name + "\");\n}\n");
					}
					else
					{
						cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_INPUT + ".deserialize("
							 + name + ",\"" + name + "\");\n");
					}
				}
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_INPUT + ".endRecord(*this,"
					 + org.apache.hadoop.record.compiler.Consts.TAG + ");\n");
				cb.append("return;\n");
				cb.append("}\n");
				// deserialize()
				cb.append("void " + this.fullName + "::deserialize(::hadoop::IArchive& " + org.apache.hadoop.record.compiler.Consts
					.RECORD_INPUT + ", const char* " + org.apache.hadoop.record.compiler.Consts.TAG 
					+ ") {\n");
				cb.append("if (NULL == p" + org.apache.hadoop.record.compiler.Consts.RTI_FILTER +
					 ") {\n");
				cb.append("deserializeWithoutFilter(" + org.apache.hadoop.record.compiler.Consts.
					RECORD_INPUT + ", " + org.apache.hadoop.record.compiler.Consts.TAG + ");\n");
				cb.append("return;\n");
				cb.append("}\n");
				cb.append("// if we're here, we need to read based on version info\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_INPUT + ".startRecord(*this,"
					 + org.apache.hadoop.record.compiler.Consts.TAG + ");\n");
				cb.append("setupRtiFields();\n");
				cb.append("for (unsigned int " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX
					 + "i=0; " + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "i<p" + org.apache.hadoop.record.compiler.Consts
					.RTI_FILTER + "->getFieldTypeInfos().size(); " + org.apache.hadoop.record.compiler.Consts
					.RIO_PREFIX + "i++) {\n");
				int ct = 0;
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.CppType>> i_8 = this.fields.GetEnumerator
					(); i_8.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.CppType
						> jf = i_8.Current;
					string name = jf.getName();
					org.apache.hadoop.record.compiler.JType.CppType type = jf.getType();
					ct++;
					if (1 != ct)
					{
						cb.append("else ");
					}
					cb.append("if (" + ct + " == p" + org.apache.hadoop.record.compiler.Consts.RTI_FILTER_FIELDS
						 + "[" + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "i]) {\n");
					if (type is org.apache.hadoop.record.compiler.JBuffer.CppBuffer)
					{
						cb.append("{\nsize_t len=0; " + org.apache.hadoop.record.compiler.Consts.RECORD_INPUT
							 + ".deserialize(" + name + ",len,\"" + name + "\");\n}\n");
					}
					else
					{
						cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_INPUT + ".deserialize("
							 + name + ",\"" + name + "\");\n");
					}
					cb.append("}\n");
				}
				if (0 != ct)
				{
					cb.append("else {\n");
					cb.append("const std::vector< ::hadoop::FieldTypeInfo* >& typeInfos = p" + org.apache.hadoop.record.compiler.Consts
						.RTI_FILTER + "->getFieldTypeInfos();\n");
					cb.append("::hadoop::Utils::skip(" + org.apache.hadoop.record.compiler.Consts.RECORD_INPUT
						 + ", typeInfos[" + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "i]->getFieldID()->c_str()"
						 + ", *(typeInfos[" + org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "i]->getTypeID()));\n"
						);
					cb.append("}\n");
				}
				cb.append("}\n");
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_INPUT + ".endRecord(*this, "
					 + org.apache.hadoop.record.compiler.Consts.TAG + ");\n");
				cb.append("}\n");
				// operator <
				cb.append("bool " + this.fullName + "::operator< (const " + this.fullName + "& peer_) const {\n"
					);
				cb.append("return (1\n");
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.CppType>> i_9 = this.fields.GetEnumerator
					(); i_9.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.CppType
						> jf = i_9.Current;
					string name = jf.getName();
					cb.append("&& (" + name + " < peer_." + name + ")\n");
				}
				cb.append(");\n");
				cb.append("}\n");
				cb.append("bool " + this.fullName + "::operator== (const " + this.fullName + "& peer_) const {\n"
					);
				cb.append("return (1\n");
				for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
					<org.apache.hadoop.record.compiler.JType.CppType>> i_10 = this.fields.GetEnumerator
					(); i_10.MoveNext(); )
				{
					org.apache.hadoop.record.compiler.JField<org.apache.hadoop.record.compiler.JType.CppType
						> jf = i_10.Current;
					string name = jf.getName();
					cb.append("&& (" + name + " == peer_." + name + ")\n");
				}
				cb.append(");\n");
				cb.append("}\n");
				cb.append("const ::std::string&" + this.fullName + "::type() const {\n");
				cb.append("static const ::std::string type_(\"" + this.name + "\");\n");
				cb.append("return type_;\n");
				cb.append("}\n");
				cb.append("const ::std::string&" + this.fullName + "::signature() const {\n");
				cb.append("static const ::std::string sig_(\"" + this._enclosing.getSignature() +
					 "\");\n");
				cb.append("return sig_;\n");
				cb.append("}\n");
				cc.write(cb.ToString());
			}

			private readonly JRecord _enclosing;
		}

		internal class CRecord : org.apache.hadoop.record.compiler.JCompType.CCompType
		{
			internal CRecord(JRecord _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly JRecord _enclosing;
		}

		private string signature;

		/// <summary>Creates a new instance of JRecord</summary>
		public JRecord(string name, System.Collections.Generic.List<org.apache.hadoop.record.compiler.JField
			<org.apache.hadoop.record.compiler.JType>> flist)
		{
			setJavaType(new org.apache.hadoop.record.compiler.JRecord.JavaRecord(this, name, 
				flist));
			setCppType(new org.apache.hadoop.record.compiler.JRecord.CppRecord(this, name, flist
				));
			setCType(new org.apache.hadoop.record.compiler.JRecord.CRecord(this));
			// precompute signature
			int idx = name.LastIndexOf('.');
			string recName = Sharpen.Runtime.substring(name, idx + 1);
			java.lang.StringBuilder sb = new java.lang.StringBuilder();
			sb.Append("L").Append(recName).Append("(");
			for (System.Collections.Generic.IEnumerator<org.apache.hadoop.record.compiler.JField
				<org.apache.hadoop.record.compiler.JType>> i = flist.GetEnumerator(); i.MoveNext
				(); )
			{
				string s = i.Current.getType().getSignature();
				sb.Append(s);
			}
			sb.Append(")");
			signature = sb.ToString();
		}

		internal override string getSignature()
		{
			return signature;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void genCppCode(java.io.FileWriter hh, java.io.FileWriter cc, System.Collections.Generic.List
			<string> options)
		{
			((org.apache.hadoop.record.compiler.JRecord.CppRecord)getCppType()).genCode(hh, cc
				, options);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void genJavaCode(string destDir, System.Collections.Generic.List
			<string> options)
		{
			((org.apache.hadoop.record.compiler.JRecord.JavaRecord)getJavaType()).genCode(destDir
				, options);
		}
	}
}
