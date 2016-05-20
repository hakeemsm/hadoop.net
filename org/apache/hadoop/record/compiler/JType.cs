using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	/// <summary>Abstract Base class for all types supported by Hadoop Record I/O.</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public abstract class JType
	{
		internal static string toCamelCase(string name)
		{
			char firstChar = name[0];
			if (char.isLowerCase(firstChar))
			{
				return string.Empty + char.toUpperCase(firstChar) + Sharpen.Runtime.substring(name
					, 1);
			}
			return name;
		}

		internal org.apache.hadoop.record.compiler.JType.JavaType javaType;

		internal org.apache.hadoop.record.compiler.JType.CppType cppType;

		internal org.apache.hadoop.record.compiler.JType.CType cType;

		internal abstract class JavaType
		{
			private string name;

			private string methodSuffix;

			private string wrapper;

			private string typeIDByteString;

			internal JavaType(JType _enclosing, string javaname, string suffix, string wrapper
				, string typeIDByteString)
			{
				this._enclosing = _enclosing;
				// points to TypeID.RIOType 
				this.name = javaname;
				this.methodSuffix = suffix;
				this.wrapper = wrapper;
				this.typeIDByteString = typeIDByteString;
			}

			internal virtual void genDecl(org.apache.hadoop.record.compiler.CodeBuffer cb, string
				 fname)
			{
				cb.append("private " + this.name + " " + fname + ";\n");
			}

			internal virtual void genStaticTypeInfo(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string fname)
			{
				cb.append(org.apache.hadoop.record.compiler.Consts.RTI_VAR + ".addField(\"" + fname
					 + "\", " + this.getTypeIDObjectString() + ");\n");
			}

			internal abstract string getTypeIDObjectString();

			internal virtual void genSetRTIFilter(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, System.Collections.Generic.IDictionary<string, int> nestedStructMap)
			{
				// do nothing by default
				return;
			}

			/*void genRtiFieldCondition(CodeBuffer cb, String fname, int ct) {
			cb.append("if ((tInfo.fieldID.equals(\"" + fname + "\")) && (typeVal ==" +
			" org.apache.hadoop.record.meta." + getTypeIDByteString() + ")) {\n");
			cb.append("rtiFilterFields[i] = " + ct + ";\n");
			cb.append("}\n");
			}
			
			void genRtiNestedFieldCondition(CodeBuffer cb, String varName, int ct) {
			cb.append("if (" + varName + ".getElementTypeID().getTypeVal() == " +
			"org.apache.hadoop.record.meta." + getTypeIDByteString() +
			") {\n");
			cb.append("rtiFilterFields[i] = " + ct + ";\n");
			cb.append("}\n");
			}*/
			internal virtual void genConstructorParam(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string fname)
			{
				cb.append("final " + this.name + " " + fname);
			}

			internal virtual void genGetSet(org.apache.hadoop.record.compiler.CodeBuffer cb, 
				string fname)
			{
				cb.append("public " + this.name + " get" + org.apache.hadoop.record.compiler.JType
					.toCamelCase(fname) + "() {\n");
				cb.append("return " + fname + ";\n");
				cb.append("}\n");
				cb.append("public void set" + org.apache.hadoop.record.compiler.JType.toCamelCase
					(fname) + "(final " + this.name + " " + fname + ") {\n");
				cb.append("this." + fname + "=" + fname + ";\n");
				cb.append("}\n");
			}

			internal virtual string getType()
			{
				return this.name;
			}

			internal virtual string getWrapperType()
			{
				return this.wrapper;
			}

			internal virtual string getMethodSuffix()
			{
				return this.methodSuffix;
			}

			internal virtual string getTypeIDByteString()
			{
				return this.typeIDByteString;
			}

			internal virtual void genWriteMethod(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string fname, string tag)
			{
				cb.append(org.apache.hadoop.record.compiler.Consts.RECORD_OUTPUT + ".write" + this
					.methodSuffix + "(" + fname + ",\"" + tag + "\");\n");
			}

			internal virtual void genReadMethod(org.apache.hadoop.record.compiler.CodeBuffer 
				cb, string fname, string tag, bool decl)
			{
				if (decl)
				{
					cb.append(this.name + " " + fname + ";\n");
				}
				cb.append(fname + "=" + org.apache.hadoop.record.compiler.Consts.RECORD_INPUT + ".read"
					 + this.methodSuffix + "(\"" + tag + "\");\n");
			}

			internal virtual void genCompareTo(org.apache.hadoop.record.compiler.CodeBuffer cb
				, string fname, string other)
			{
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret = (" + fname
					 + " == " + other + ")? 0 :((" + fname + "<" + other + ")?-1:1);\n");
			}

			internal abstract void genCompareBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb);

			internal abstract void genSlurpBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string b, string s, string l);

			internal virtual void genEquals(org.apache.hadoop.record.compiler.CodeBuffer cb, 
				string fname, string peer)
			{
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret = (" + fname
					 + "==" + peer + ");\n");
			}

			internal virtual void genHashCode(org.apache.hadoop.record.compiler.CodeBuffer cb
				, string fname)
			{
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret = (int)" + fname
					 + ";\n");
			}

			internal virtual void genConstructorSet(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string fname)
			{
				cb.append("this." + fname + " = " + fname + ";\n");
			}

			internal virtual void genClone(org.apache.hadoop.record.compiler.CodeBuffer cb, string
				 fname)
			{
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "other." + fname 
					+ " = this." + fname + ";\n");
			}

			private readonly JType _enclosing;
		}

		internal abstract class CppType
		{
			private string name;

			internal CppType(JType _enclosing, string cppname)
			{
				this._enclosing = _enclosing;
				this.name = cppname;
			}

			internal virtual void genDecl(org.apache.hadoop.record.compiler.CodeBuffer cb, string
				 fname)
			{
				cb.append(this.name + " " + fname + ";\n");
			}

			internal virtual void genStaticTypeInfo(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string fname)
			{
				cb.append("p->addField(new ::std::string(\"" + fname + "\"), " + this.getTypeIDObjectString
					() + ");\n");
			}

			internal virtual void genGetSet(org.apache.hadoop.record.compiler.CodeBuffer cb, 
				string fname)
			{
				cb.append("virtual " + this.name + " get" + org.apache.hadoop.record.compiler.JType
					.toCamelCase(fname) + "() const {\n");
				cb.append("return " + fname + ";\n");
				cb.append("}\n");
				cb.append("virtual void set" + org.apache.hadoop.record.compiler.JType.toCamelCase
					(fname) + "(" + this.name + " m_) {\n");
				cb.append(fname + "=m_;\n");
				cb.append("}\n");
			}

			internal abstract string getTypeIDObjectString();

			internal virtual void genSetRTIFilter(org.apache.hadoop.record.compiler.CodeBuffer
				 cb)
			{
				// do nothing by default
				return;
			}

			internal virtual string getType()
			{
				return this.name;
			}

			private readonly JType _enclosing;
		}

		internal class CType
		{
			internal CType(JType _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly JType _enclosing;
		}

		internal abstract string getSignature();

		internal virtual void setJavaType(org.apache.hadoop.record.compiler.JType.JavaType
			 jType)
		{
			this.javaType = jType;
		}

		internal virtual org.apache.hadoop.record.compiler.JType.JavaType getJavaType()
		{
			return javaType;
		}

		internal virtual void setCppType(org.apache.hadoop.record.compiler.JType.CppType 
			cppType)
		{
			this.cppType = cppType;
		}

		internal virtual org.apache.hadoop.record.compiler.JType.CppType getCppType()
		{
			return cppType;
		}

		internal virtual void setCType(org.apache.hadoop.record.compiler.JType.CType cType
			)
		{
			this.cType = cType;
		}

		internal virtual org.apache.hadoop.record.compiler.JType.CType getCType()
		{
			return cType;
		}
	}
}
