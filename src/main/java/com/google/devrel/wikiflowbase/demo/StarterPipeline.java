/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.devrel.wikiflowbase.demo;

import io.DoFirebasePush;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.FirebaseAuthenticator;
import utils.FirebaseEmptyAuthenticator;

import com.firebase.client.Firebase;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * A starter example for writing Google Cloud Dataflow programs.
 *
 * <p>
 * The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>
 * To run this starter example locally using DirectPipelineRunner, just execute
 * it without any additional parameters from your favorite development
 * environment. In Eclipse, this corresponds to the existing 'LOCAL' run
 * configuration.
 *
 * <p>
 * To run this starter example using managed resource in Google Cloud Platform,
 * you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 * --runner=BlockingDataflowPipelineRunner In Eclipse, you can just modify the
 * existing 'SERVICE' run configuration.
 */
@SuppressWarnings("serial")
public class StarterPipeline {
	private static final Logger LOG = LoggerFactory
			.getLogger(StarterPipeline.class);

	public static void main(String[] args) {
		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args)
				.withValidation().create());

		Firebase testRef = new Firebase(
				"https://dataflowio.firebaseio-demo.com").child("demo"
				+ System.currentTimeMillis());
		FirebaseAuthenticator auther = new FirebaseEmptyAuthenticator();

		PCollection<String> helloWordP = p.apply(Create.of("Hello", "World"));
		helloWordP.apply(ParDo.of(new DoFn<String, String>() {
			@Override
			public void processElement(ProcessContext c) {
				c.output(c.element().toUpperCase());
			}
		})).apply(ParDo.of(new DoFn<String, Void>() {
			@Override
			public void processElement(ProcessContext c) {
				LOG.info(c.element());
			}
		}));

		helloWordP
				.apply(ParDo.of(new DoFirebasePush(testRef.toString(), auther)));
		p.run();
	}
}
