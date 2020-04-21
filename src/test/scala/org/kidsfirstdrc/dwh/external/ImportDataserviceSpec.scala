package org.kidsfirstdrc.dwh.external

import org.kidsfirstdrc.dwh.testutils.WithSparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ImportDataserviceSpec extends AnyFlatSpec with GivenWhenThen with WithSparkSession with Matchers {

  "parseArgs" should "pars all args" in {
    ImportDataservice.parseArgs(Array("study1", "release1", "input", "output", "true")) shouldBe (Set("study1"), "release1","input", "output", true, Dataservice.ALL_TABLES)
    ImportDataservice.parseArgs(Array("study1", "release1", "input", "output", "false")) shouldBe (Set("study1"), "release1","input", "output", false, Dataservice.ALL_TABLES)
    ImportDataservice.parseArgs(Array("study1,study2", "release1", "input", "output", "true")) shouldBe (Set("study1", "study2"), "release1","input", "output", true, Dataservice.ALL_TABLES)
    ImportDataservice.parseArgs(Array("study1", "release1", "input", "output", "false", "participants,biospecimens")) shouldBe (Set("study1"), "release1","input", "output", false, Set("participants","biospecimens"))
    ImportDataservice.parseArgs(Array("study1", "release1", "input", "output", "false", "all")) shouldBe (Set("study1"), "release1","input", "output", false, Dataservice.ALL_TABLES)
  }

}
