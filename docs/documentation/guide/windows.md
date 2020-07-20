---
title: Windows and triggers
layout: default
nav_order: 5
parent: Developers guide
---

# Windows and triggers
{: .no_toc }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

## Overview

Windows split the stream into some "blocks" of finite size and all transformations over the stream will be applied to
these "blocks" of the data. In this section you can find base information about window operations, to read more about
windows and triggers in the Apache Beam's [programming guide](https://beam.apache.org/documentation/programming-guide/#windowing).

## Windowing methods

| Method | Description |
|:--|:--|
| **withWindow(<br />&nbsp;&nbsp;&nbsp;&nbsp;_windowFn_,<br />&nbsp;&nbsp;&nbsp;&nbsp;_options_<br />)** | Applies defined window function with specified options to the input stream.<br /><br />_Arguments:_<br />·&nbsp;`windowFn: WindowFn<T, *>` - a window function<br />·&nbsp;`options: Configurator<WindowFn<T>>` - additional configurations for the window function |
| **withWindowByDays(<br />&nbsp;&nbsp;&nbsp;&nbsp;_number_,<br />&nbsp;&nbsp;&nbsp;&nbsp;_options_<br />)** | Applies the window function to combine elements into periods measured by days.<br /><br />_Arguments:_<br />·&nbsp;`number: Int` - a number of days<br />·&nbsp;`options: Configurator<WindowFn<T>>` - additional configurations for the window function |
| **withWindowByWeeks(<br />&nbsp;&nbsp;&nbsp;&nbsp;_number_,<br />&nbsp;&nbsp;&nbsp;&nbsp;_startDayOfWeek_,<br />&nbsp;&nbsp;&nbsp;&nbsp;_options_<br />)** | Applies the window function to combine elements into periods measured by weeks.<br /><br />_Arguments:_<br />·&nbsp;`number: Int` - a number of weeks<br />·&nbsp;`startDayOfWeek: Int` - the day of week to start, where `1` is Monday and `7` is Sunday<br />·&nbsp;`options: Configurator<WindowFn<T>>` - additional configurations for the window function |
| **withWindowByMonths(<br />&nbsp;&nbsp;&nbsp;&nbsp;_number_,<br />&nbsp;&nbsp;&nbsp;&nbsp;_options_<br />)** | Applies the window function to combine elements into periods measured by months.<br /><br />_Arguments:_<br />·&nbsp;`number: Int` - a number of months<br />·&nbsp;`options: Configurator<WindowFn<T>>` - additional configurations for the window function |
| **withWindowByYears(<br />&nbsp;&nbsp;&nbsp;&nbsp;_number_,<br />&nbsp;&nbsp;&nbsp;&nbsp;_options_<br />)** | Applies the window function to combine elements into periods measured by years.<br /><br />_Arguments:_<br />·&nbsp;`number: Int` - a number of years<br />·&nbsp;`options: Configurator<WindowFn<T>>` - additional configurations for the window function |
| **withGlobalWindow(<br />&nbsp;&nbsp;&nbsp;&nbsp;_options_<br />)** | Combines all input elements into one common window.<br /><br />_Arguments:_<br />·&nbsp;`options: Configurator<WindowFn<T>>` - additional configurations for the window function |
| **withFixedWindow(<br />&nbsp;&nbsp;&nbsp;&nbsp;_duration_,<br />&nbsp;&nbsp;&nbsp;&nbsp;_offset_,<br />&nbsp;&nbsp;&nbsp;&nbsp;_options_<br />)** | Applies the window function to combine elements into fixed-size timestamp-based windows.<br /><br />_Arguments:_<br />·&nbsp;`duration: Duration` - the size of windows<br />·&nbsp;`offset: Duration` - offset before the start of windows (default: `Duration.ZERO`)<br />·&nbsp;`options: Configurator<WindowFn<T>>` - additional configurations for the window function |
| **withSessionWindows(<br />&nbsp;&nbsp;&nbsp;&nbsp;_gap_,<br />&nbsp;&nbsp;&nbsp;&nbsp;_options_<br />)** | Applies the window function to combine elements into sessions separated by periods with no input for at least the `gap` duration.<br /><br />_Arguments:_<br />·&nbsp;`gap: Duration` - the size of the gap between sessions<br />·&nbsp;`options: Configurator<WindowFn<T>>` - additional configurations for the window function |
| **withSlidingWindow(<br />&nbsp;&nbsp;&nbsp;&nbsp;_size_,<br />&nbsp;&nbsp;&nbsp;&nbsp;_period_,<br />&nbsp;&nbsp;&nbsp;&nbsp;_offset_,<br />&nbsp;&nbsp;&nbsp;&nbsp;_options_<br />)** | Applies the window function to combine elements into possibly overlapping fixed-size timestamp-based windows.<br /><br />_Arguments:_<br />·&nbsp;`duration: Duration` - the size of windows<br />·&nbsp;`period: Duration` - a period between windows<br />·&nbsp;`offset: Duration` - offset before the start of windows (default: `Duration.ZERO`)<br />·&nbsp;`options: Configurator<WindowFn<T>>` - additional configurations for the window function |
