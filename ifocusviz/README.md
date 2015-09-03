IFOCUSVIZ
=========

An adaptation of the IFOCUS algorithm by Blais, et al. (2015) to exploit
visual properties for rapid sampling.

Specifically, we use the (probably, user-specified) encoding and perceptual
functions to obtain the margin of error for each approximate answer while
ensuring ordering guarantees for certain aggregated values.

As a control, we have implemented the original IFOCUS algorithm that is
oblivious to the encoding and perceptual functions. The control is used to
compare IFOCUSVIZ's sample complexity runtime performance.

Requires:
* Apache Spark
* Scala
* JDK 7 (using `java.nio.file` package`)

To run:
* First package the project as a jar using `sbt package` in the `ifocus/spark` directory.
* Then run one of the `*Estimator` classes:
`ExactEstimator`, `IFocusEstimator`, or ``
For example, to run `IFocusEstimator` on the January 2015 - April 2015 datasets use:
```
$SPARK_HOME/bin/spark-submit --class "IFocusEstimator" --master "local[4]" target/scala-2.10/ifocusviz_2.10-1.0.jar "../data/cleaned_states_dep_delay_jan.csv,../data/cleaned_states_dep_delay_feb.csv,../data/cleaned_states_dep_delay_mar.csv,../data/cleaned_states_dep_delay_apr.csv"
```
The command above assumes you have set the environment variable `$SPARK_HOME` to where you
locally installed your spark distribution.
