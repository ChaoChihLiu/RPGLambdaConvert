# CPBPC TTS Service

here is the source code of Lambda Java, generate audio with SSML script, auto-merge clips

## Prerequisites
1. openJDK 11
2. gradle-7.5.1

## How to compile
* generate lambda layer: ./gradlew clean buildZip
* compile: ./gradlew clean build

## Lambda explanation
Naming convention is a bit confused...because all of these functions are added progressively...
* com.cpbpc.rpgv2.RPGLambda: English RPG
* com.cpbpc.rpgv2.CRPGLambda: Chinese RPG, KJV and CUVS
* com.cpbpc.rpgv2.RPGAudioMerger: manually merge RPG audio
* com.cpbpc.remembrance.AudioMerger: manually merge daily remembrance
* com.cpbpc.bible.BibleAudioMerger: manually merge KJV and CUVS


* com.cpbpc.rpgv2.AutoMerger: Automatically merge clips to final audio, including RPG
* com.cpbpc.bible.AutoMerger: Automatically merge clips to final audio, including KJV and CUVS
* com.cpbpc.remembrance.AutoMerger: Automatically merge clips to final audio, including Daily Remembrance
