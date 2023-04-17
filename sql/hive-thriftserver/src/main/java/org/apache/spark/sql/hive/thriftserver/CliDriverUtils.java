/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Splitter;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.ArgumentCompleter.ArgumentDelimiter;
import jline.console.completer.ArgumentCompleter.AbstractArgumentDelimiter;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.Validator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.HiveParser;

import static org.apache.hadoop.hive.cli.CliDriver.DELIMITED_CANDIDATE_THRESHOLD;

public class CliDriverUtils {

  public static Completer[] getCommandCompleter() {
    // StringsCompleter matches against a pre-defined wordlist
    // We start with an empty wordlist and build it up
    List<String> candidateStrings = new ArrayList<String>();

    // We add Hive function names
    // For functions that aren't infix operators, we add an open
    // parenthesis at the end.
    for (String s : FunctionRegistry.getFunctionNames()) {
      if (s.matches("[a-z_]+")) {
        candidateStrings.add(s + "(");
      } else {
        candidateStrings.add(s);
      }
    }

    // We add Hive keywords, including lower-cased versions
    for (String s : HiveParser.getKeywords()) {
      candidateStrings.add(s);
      candidateStrings.add(s.toLowerCase());
    }

    StringsCompleter strCompleter = new StringsCompleter(candidateStrings);

    // Because we use parentheses in addition to whitespace
    // as a keyword delimiter, we need to define a new ArgumentDelimiter
    // that recognizes parenthesis as a delimiter.
    ArgumentDelimiter delim = new AbstractArgumentDelimiter() {
      @Override
      public boolean isDelimiterChar(CharSequence buffer, int pos) {
        char c = buffer.charAt(pos);
        return (Character.isWhitespace(c) || c == '(' || c == ')' ||
            c == '[' || c == ']');
      }
    };

    // The ArgumentCompletor allows us to match multiple tokens
    // in the same line.
    final ArgumentCompleter argCompleter = new ArgumentCompleter(delim, strCompleter);
    // By default ArgumentCompletor is in "strict" mode meaning
    // a token is only auto-completed if all prior tokens
    // match. We don't want that since there are valid tokens
    // that are not in our wordlist (eg. table and column names)
    argCompleter.setStrict(false);

    // ArgumentCompletor always adds a space after a matched token.
    // This is undesirable for function names because a space after
    // the opening parenthesis is unnecessary (and uncommon) in Hive.
    // We stack a custom Completor on top of our ArgumentCompletor
    // to reverse this.
    Completer customCompletor = new Completer () {
      @Override
      public int complete (String buffer, int offset, List completions) {
        List<String> comp = completions;
        int ret = argCompleter.complete(buffer, offset, completions);
        // ConsoleReader will do the substitution if and only if there
        // is exactly one valid completion, so we ignore other cases.
        if (completions.size() == 1) {
          if (comp.get(0).endsWith("( ")) {
            comp.set(0, comp.get(0).trim());
          }
        }
        return ret;
      }
    };

    List<String> vars = new ArrayList<String>();
    for (HiveConf.ConfVars conf : HiveConf.ConfVars.values()) {
      vars.add(conf.varname);
    }

    StringsCompleter confCompleter = new StringsCompleter(vars) {
      @Override
      public int complete(final String buffer, final int cursor, final List<CharSequence> clist) {
        int result = super.complete(buffer, cursor, clist);
        if (clist.isEmpty() && cursor > 1 && buffer.charAt(cursor - 1) == '=') {
          HiveConf.ConfVars var = HiveConf.getConfVars(buffer.substring(0, cursor - 1));
          if (var == null) {
            return result;
          }
          if (var.getValidator() instanceof Validator.StringSet) {
            Validator.StringSet validator = (Validator.StringSet)var.getValidator();
            clist.addAll(validator.getExpected());
          } else if (var.getValidator() != null) {
            clist.addAll(Arrays.asList(var.getValidator().toDescription(), ""));
          } else {
            clist.addAll(Arrays.asList("Expects " + var.typeString() + " type value", ""));
          }
          return cursor;
        }
        if (clist.size() > DELIMITED_CANDIDATE_THRESHOLD) {
          Set<CharSequence> delimited = new LinkedHashSet<CharSequence>();
          for (CharSequence candidate : clist) {
            Iterator<String> it = Splitter.on(".").split(
                candidate.subSequence(cursor, candidate.length())).iterator();
            if (it.hasNext()) {
              String next = it.next();
              if (next.isEmpty()) {
                next = ".";
              }
              candidate = buffer != null ? buffer.substring(0, cursor) + next : next;
            }
            delimited.add(candidate);
          }
          clist.clear();
          clist.addAll(delimited);
        }
        return result;
      }
    };

    StringsCompleter setCompleter = new StringsCompleter("set") {
      @Override
      public int complete(String buffer, int cursor, List<CharSequence> clist) {
        return buffer != null && buffer.equals("set") ? super.complete(buffer, cursor, clist) : -1;
      }
    };

    ArgumentCompleter propCompleter = new ArgumentCompleter(setCompleter, confCompleter) {
      @Override
      public int complete(String buffer, int offset, List<CharSequence> completions) {
        int ret = super.complete(buffer, offset, completions);
        if (completions.size() == 1) {
          completions.set(0, ((String)completions.get(0)).trim());
        }
        return ret;
      }
    };
    return new Completer[] {propCompleter, customCompletor};
  }

}
