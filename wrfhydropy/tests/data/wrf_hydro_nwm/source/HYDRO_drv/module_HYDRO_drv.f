












!  Program Name:
!  Author(s)/Contact(s):
!  Abstract:
!  History Log:
! 
!  Usage:
!  Parameters: <Specify typical arguments passed>
!  Input Files:
!        <list file names and briefly describe the data they include>
!  Output Files:
!        <list file names and briefly describe the information they include>
! 
!  Condition codes:
!        <list exit condition or error codes returned >
!        If appropriate, descriptive troubleshooting instructions or
!        likely causes for failures could be mentioned here with the
!        appropriate error code
! 
!  User controllable options: <if applicable>

module module_HYDRO_drv
   use module_HYDRO_io, only:  output_rt, mpp_output_chrt, mpp_output_lakes, mpp_output_chrtgrd, &
                               restart_out_bi, restart_in_bi, mpp_output_chrt2, mpp_output_lakes2, &
                               hdtbl_in_nc, hdtbl_out
   USE module_mpp_land
   use module_NWM_io, only: output_chrt_NWM, output_rt_NWM, output_lakes_NWM,&
                            output_chrtout_grd_NWM, output_lsmOut_NWM, &
                            output_frxstPts, output_chanObs_NWM, output_gw_NWM
   use module_HYDRO_io, only: sub_output_gw, restart_out_nc, restart_in_nc,  &
        get_file_dimension , get_file_globalatts, get2d_lsm_real, get2d_lsm_vegtyp, get2d_lsm_soltyp, &
        output_lsm,  output_GW_Diag
   use module_HYDRO_io, only : output_lakes2
   use module_rt_data, only: rt_domain
   use module_GW_baseflow
   use module_gw_gw2d
   use module_gw_gw2d_data, only: gw2d
   use module_channel_routing, only: drive_channel, drive_channel_rsl
   use module_namelist, only: nlst_rt, read_rt_nlst
   use module_routing, only: getChanDim, landrt_ini
   use module_HYDRO_utils
!   use module_namelist
   use module_lsm_forcing, only: geth_newdate
   use module_stream_nudging,  only: init_stream_nudging

   use module_UDMAP, only: get_basn_area_nhd
   use netcdf
   
   implicit none

!     NetCDF-3.
!
! netcdf version 3 fortran interface:
!

!
! external netcdf data types:
!
      integer nf_byte
      integer nf_int1
      integer nf_char
      integer nf_short
      integer nf_int2
      integer nf_int
      integer nf_float
      integer nf_real
      integer nf_double

      parameter (nf_byte = 1)
      parameter (nf_int1 = nf_byte)
      parameter (nf_char = 2)
      parameter (nf_short = 3)
      parameter (nf_int2 = nf_short)
      parameter (nf_int = 4)
      parameter (nf_float = 5)
      parameter (nf_real = nf_float)
      parameter (nf_double = 6)

!
! default fill values:
!
      integer           nf_fill_byte
      integer           nf_fill_int1
      integer           nf_fill_char
      integer           nf_fill_short
      integer           nf_fill_int2
      integer           nf_fill_int
      real              nf_fill_float
      real              nf_fill_real
      doubleprecision   nf_fill_double

      parameter (nf_fill_byte = -127)
      parameter (nf_fill_int1 = nf_fill_byte)
      parameter (nf_fill_char = 0)
      parameter (nf_fill_short = -32767)
      parameter (nf_fill_int2 = nf_fill_short)
      parameter (nf_fill_int = -2147483647)
      parameter (nf_fill_float = 9.9692099683868690e+36)
      parameter (nf_fill_real = nf_fill_float)
      parameter (nf_fill_double = 9.9692099683868690d+36)

!
! mode flags for opening and creating a netcdf dataset:
!
      integer nf_nowrite
      integer nf_write
      integer nf_clobber
      integer nf_noclobber
      integer nf_fill
      integer nf_nofill
      integer nf_lock
      integer nf_share
      integer nf_64bit_offset
      integer nf_sizehint_default
      integer nf_align_chunk
      integer nf_format_classic
      integer nf_format_64bit
      integer nf_diskless
      integer nf_mmap

      parameter (nf_nowrite = 0)
      parameter (nf_write = 1)
      parameter (nf_clobber = 0)
      parameter (nf_noclobber = 4)
      parameter (nf_fill = 0)
      parameter (nf_nofill = 256)
      parameter (nf_lock = 1024)
      parameter (nf_share = 2048)
      parameter (nf_64bit_offset = 512)
      parameter (nf_sizehint_default = 0)
      parameter (nf_align_chunk = -1)
      parameter (nf_format_classic = 1)
      parameter (nf_format_64bit = 2)
      parameter (nf_diskless = 8)
      parameter (nf_mmap = 16)

!
! size argument for defining an unlimited dimension:
!
      integer nf_unlimited
      parameter (nf_unlimited = 0)

!
! global attribute id:
!
      integer nf_global
      parameter (nf_global = 0)

!
! implementation limits:
!
      integer nf_max_dims
      integer nf_max_attrs
      integer nf_max_vars
      integer nf_max_name
      integer nf_max_var_dims

      parameter (nf_max_dims = 1024)
      parameter (nf_max_attrs = 8192)
      parameter (nf_max_vars = 8192)
      parameter (nf_max_name = 256)
      parameter (nf_max_var_dims = nf_max_dims)

!
! error codes:
!
      integer nf_noerr
      integer nf_ebadid
      integer nf_eexist
      integer nf_einval
      integer nf_eperm
      integer nf_enotindefine
      integer nf_eindefine
      integer nf_einvalcoords
      integer nf_emaxdims
      integer nf_enameinuse
      integer nf_enotatt
      integer nf_emaxatts
      integer nf_ebadtype
      integer nf_ebaddim
      integer nf_eunlimpos
      integer nf_emaxvars
      integer nf_enotvar
      integer nf_eglobal
      integer nf_enotnc
      integer nf_ests
      integer nf_emaxname
      integer nf_eunlimit
      integer nf_enorecvars
      integer nf_echar
      integer nf_eedge
      integer nf_estride
      integer nf_ebadname
      integer nf_erange
      integer nf_enomem
      integer nf_evarsize
      integer nf_edimsize
      integer nf_etrunc

      parameter (nf_noerr = 0)
      parameter (nf_ebadid = -33)
      parameter (nf_eexist = -35)
      parameter (nf_einval = -36)
      parameter (nf_eperm = -37)
      parameter (nf_enotindefine = -38)
      parameter (nf_eindefine = -39)
      parameter (nf_einvalcoords = -40)
      parameter (nf_emaxdims = -41)
      parameter (nf_enameinuse = -42)
      parameter (nf_enotatt = -43)
      parameter (nf_emaxatts = -44)
      parameter (nf_ebadtype = -45)
      parameter (nf_ebaddim = -46)
      parameter (nf_eunlimpos = -47)
      parameter (nf_emaxvars = -48)
      parameter (nf_enotvar = -49)
      parameter (nf_eglobal = -50)
      parameter (nf_enotnc = -51)
      parameter (nf_ests = -52)
      parameter (nf_emaxname = -53)
      parameter (nf_eunlimit = -54)
      parameter (nf_enorecvars = -55)
      parameter (nf_echar = -56)
      parameter (nf_eedge = -57)
      parameter (nf_estride = -58)
      parameter (nf_ebadname = -59)
      parameter (nf_erange = -60)
      parameter (nf_enomem = -61)
      parameter (nf_evarsize = -62)
      parameter (nf_edimsize = -63)
      parameter (nf_etrunc = -64)
!
! error handling modes:
!
      integer  nf_fatal
      integer nf_verbose

      parameter (nf_fatal = 1)
      parameter (nf_verbose = 2)

!
! miscellaneous routines:
!
      character*80   nf_inq_libvers
      external       nf_inq_libvers

      character*80   nf_strerror
!                         (integer             ncerr)
      external       nf_strerror

      logical        nf_issyserr
!                         (integer             ncerr)
      external       nf_issyserr

!
! control routines:
!
      integer         nf_inq_base_pe
!                         (integer             ncid,
!                          integer             pe)
      external        nf_inq_base_pe

      integer         nf_set_base_pe
!                         (integer             ncid,
!                          integer             pe)
      external        nf_set_base_pe

      integer         nf_create
!                         (character*(*)       path,
!                          integer             cmode,
!                          integer             ncid)
      external        nf_create

      integer         nf__create
!                         (character*(*)       path,
!                          integer             cmode,
!                          integer             initialsz,
!                          integer             chunksizehint,
!                          integer             ncid)
      external        nf__create

      integer         nf__create_mp
!                         (character*(*)       path,
!                          integer             cmode,
!                          integer             initialsz,
!                          integer             basepe,
!                          integer             chunksizehint,
!                          integer             ncid)
      external        nf__create_mp

      integer         nf_open
!                         (character*(*)       path,
!                          integer             mode,
!                          integer             ncid)
      external        nf_open

      integer         nf__open
!                         (character*(*)       path,
!                          integer             mode,
!                          integer             chunksizehint,
!                          integer             ncid)
      external        nf__open

      integer         nf__open_mp
!                         (character*(*)       path,
!                          integer             mode,
!                          integer             basepe,
!                          integer             chunksizehint,
!                          integer             ncid)
      external        nf__open_mp

      integer         nf_set_fill
!                         (integer             ncid,
!                          integer             fillmode,
!                          integer             old_mode)
      external        nf_set_fill

      integer         nf_set_default_format
!                          (integer             format,
!                          integer             old_format)
      external        nf_set_default_format

      integer         nf_redef
!                         (integer             ncid)
      external        nf_redef

      integer         nf_enddef
!                         (integer             ncid)
      external        nf_enddef

      integer         nf__enddef
!                         (integer             ncid,
!                          integer             h_minfree,
!                          integer             v_align,
!                          integer             v_minfree,
!                          integer             r_align)
      external        nf__enddef

      integer         nf_sync
!                         (integer             ncid)
      external        nf_sync

      integer         nf_abort
!                         (integer             ncid)
      external        nf_abort

      integer         nf_close
!                         (integer             ncid)
      external        nf_close

      integer         nf_delete
!                         (character*(*)       ncid)
      external        nf_delete

!
! general inquiry routines:
!

      integer         nf_inq
!                         (integer             ncid,
!                          integer             ndims,
!                          integer             nvars,
!                          integer             ngatts,
!                          integer             unlimdimid)
      external        nf_inq

! new inquire path

      integer nf_inq_path
      external nf_inq_path

      integer         nf_inq_ndims
!                         (integer             ncid,
!                          integer             ndims)
      external        nf_inq_ndims

      integer         nf_inq_nvars
!                         (integer             ncid,
!                          integer             nvars)
      external        nf_inq_nvars

      integer         nf_inq_natts
!                         (integer             ncid,
!                          integer             ngatts)
      external        nf_inq_natts

      integer         nf_inq_unlimdim
!                         (integer             ncid,
!                          integer             unlimdimid)
      external        nf_inq_unlimdim

      integer         nf_inq_format
!                         (integer             ncid,
!                          integer             format)
      external        nf_inq_format

!
! dimension routines:
!

      integer         nf_def_dim
!                         (integer             ncid,
!                          character(*)        name,
!                          integer             len,
!                          integer             dimid)
      external        nf_def_dim

      integer         nf_inq_dimid
!                         (integer             ncid,
!                          character(*)        name,
!                          integer             dimid)
      external        nf_inq_dimid

      integer         nf_inq_dim
!                         (integer             ncid,
!                          integer             dimid,
!                          character(*)        name,
!                          integer             len)
      external        nf_inq_dim

      integer         nf_inq_dimname
!                         (integer             ncid,
!                          integer             dimid,
!                          character(*)        name)
      external        nf_inq_dimname

      integer         nf_inq_dimlen
!                         (integer             ncid,
!                          integer             dimid,
!                          integer             len)
      external        nf_inq_dimlen

      integer         nf_rename_dim
!                         (integer             ncid,
!                          integer             dimid,
!                          character(*)        name)
      external        nf_rename_dim

!
! general attribute routines:
!

      integer         nf_inq_att
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             xtype,
!                          integer             len)
      external        nf_inq_att

      integer         nf_inq_attid
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             attnum)
      external        nf_inq_attid

      integer         nf_inq_atttype
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             xtype)
      external        nf_inq_atttype

      integer         nf_inq_attlen
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             len)
      external        nf_inq_attlen

      integer         nf_inq_attname
!                         (integer             ncid,
!                          integer             varid,
!                          integer             attnum,
!                          character(*)        name)
      external        nf_inq_attname

      integer         nf_copy_att
!                         (integer             ncid_in,
!                          integer             varid_in,
!                          character(*)        name,
!                          integer             ncid_out,
!                          integer             varid_out)
      external        nf_copy_att

      integer         nf_rename_att
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        curname,
!                          character(*)        newname)
      external        nf_rename_att

      integer         nf_del_att
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name)
      external        nf_del_att

!
! attribute put/get routines:
!

      integer         nf_put_att_text
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             len,
!                          character(*)        text)
      external        nf_put_att_text

      integer         nf_get_att_text
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          character(*)        text)
      external        nf_get_att_text

      integer         nf_put_att_int1
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             xtype,
!                          integer             len,
!                          nf_int1_t           i1vals(1))
      external        nf_put_att_int1

      integer         nf_get_att_int1
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          nf_int1_t           i1vals(1))
      external        nf_get_att_int1

      integer         nf_put_att_int2
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             xtype,
!                          integer             len,
!                          nf_int2_t           i2vals(1))
      external        nf_put_att_int2

      integer         nf_get_att_int2
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          nf_int2_t           i2vals(1))
      external        nf_get_att_int2

      integer         nf_put_att_int
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             xtype,
!                          integer             len,
!                          integer             ivals(1))
      external        nf_put_att_int

      integer         nf_get_att_int
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             ivals(1))
      external        nf_get_att_int

      integer         nf_put_att_real
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             xtype,
!                          integer             len,
!                          real                rvals(1))
      external        nf_put_att_real

      integer         nf_get_att_real
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          real                rvals(1))
      external        nf_get_att_real

      integer         nf_put_att_double
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             xtype,
!                          integer             len,
!                          double              dvals(1))
      external        nf_put_att_double

      integer         nf_get_att_double
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          double              dvals(1))
      external        nf_get_att_double

!
! general variable routines:
!

      integer         nf_def_var
!                         (integer             ncid,
!                          character(*)        name,
!                          integer             datatype,
!                          integer             ndims,
!                          integer             dimids(1),
!                          integer             varid)
      external        nf_def_var

      integer         nf_inq_var
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             datatype,
!                          integer             ndims,
!                          integer             dimids(1),
!                          integer             natts)
      external        nf_inq_var

      integer         nf_inq_varid
!                         (integer             ncid,
!                          character(*)        name,
!                          integer             varid)
      external        nf_inq_varid

      integer         nf_inq_varname
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name)
      external        nf_inq_varname

      integer         nf_inq_vartype
!                         (integer             ncid,
!                          integer             varid,
!                          integer             xtype)
      external        nf_inq_vartype

      integer         nf_inq_varndims
!                         (integer             ncid,
!                          integer             varid,
!                          integer             ndims)
      external        nf_inq_varndims

      integer         nf_inq_vardimid
!                         (integer             ncid,
!                          integer             varid,
!                          integer             dimids(1))
      external        nf_inq_vardimid

      integer         nf_inq_varnatts
!                         (integer             ncid,
!                          integer             varid,
!                          integer             natts)
      external        nf_inq_varnatts

      integer         nf_rename_var
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name)
      external        nf_rename_var

      integer         nf_copy_var
!                         (integer             ncid_in,
!                          integer             varid,
!                          integer             ncid_out)
      external        nf_copy_var

!
! entire variable put/get routines:
!

      integer         nf_put_var_text
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        text)
      external        nf_put_var_text

      integer         nf_get_var_text
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        text)
      external        nf_get_var_text

      integer         nf_put_var_int1
!                         (integer             ncid,
!                          integer             varid,
!                          nf_int1_t           i1vals(1))
      external        nf_put_var_int1

      integer         nf_get_var_int1
!                         (integer             ncid,
!                          integer             varid,
!                          nf_int1_t           i1vals(1))
      external        nf_get_var_int1

      integer         nf_put_var_int2
!                         (integer             ncid,
!                          integer             varid,
!                          nf_int2_t           i2vals(1))
      external        nf_put_var_int2

      integer         nf_get_var_int2
!                         (integer             ncid,
!                          integer             varid,
!                          nf_int2_t           i2vals(1))
      external        nf_get_var_int2

      integer         nf_put_var_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             ivals(1))
      external        nf_put_var_int

      integer         nf_get_var_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             ivals(1))
      external        nf_get_var_int

      integer         nf_put_var_real
!                         (integer             ncid,
!                          integer             varid,
!                          real                rvals(1))
      external        nf_put_var_real

      integer         nf_get_var_real
!                         (integer             ncid,
!                          integer             varid,
!                          real                rvals(1))
      external        nf_get_var_real

      integer         nf_put_var_double
!                         (integer             ncid,
!                          integer             varid,
!                          doubleprecision     dvals(1))
      external        nf_put_var_double

      integer         nf_get_var_double
!                         (integer             ncid,
!                          integer             varid,
!                          doubleprecision     dvals(1))
      external        nf_get_var_double

!
! single variable put/get routines:
!

      integer         nf_put_var1_text
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          character*1         text)
      external        nf_put_var1_text

      integer         nf_get_var1_text
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          character*1         text)
      external        nf_get_var1_text

      integer         nf_put_var1_int1
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          nf_int1_t           i1val)
      external        nf_put_var1_int1

      integer         nf_get_var1_int1
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          nf_int1_t           i1val)
      external        nf_get_var1_int1

      integer         nf_put_var1_int2
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          nf_int2_t           i2val)
      external        nf_put_var1_int2

      integer         nf_get_var1_int2
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          nf_int2_t           i2val)
      external        nf_get_var1_int2

      integer         nf_put_var1_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          integer             ival)
      external        nf_put_var1_int

      integer         nf_get_var1_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          integer             ival)
      external        nf_get_var1_int

      integer         nf_put_var1_real
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          real                rval)
      external        nf_put_var1_real

      integer         nf_get_var1_real
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          real                rval)
      external        nf_get_var1_real

      integer         nf_put_var1_double
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          doubleprecision     dval)
      external        nf_put_var1_double

      integer         nf_get_var1_double
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          doubleprecision     dval)
      external        nf_get_var1_double

!
! variable array put/get routines:
!

      integer         nf_put_vara_text
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          character(*)        text)
      external        nf_put_vara_text

      integer         nf_get_vara_text
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          character(*)        text)
      external        nf_get_vara_text

      integer         nf_put_vara_int1
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          nf_int1_t           i1vals(1))
      external        nf_put_vara_int1

      integer         nf_get_vara_int1
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          nf_int1_t           i1vals(1))
      external        nf_get_vara_int1

      integer         nf_put_vara_int2
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          nf_int2_t           i2vals(1))
      external        nf_put_vara_int2

      integer         nf_get_vara_int2
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          nf_int2_t           i2vals(1))
      external        nf_get_vara_int2

      integer         nf_put_vara_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             ivals(1))
      external        nf_put_vara_int

      integer         nf_get_vara_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             ivals(1))
      external        nf_get_vara_int

      integer         nf_put_vara_real
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          real                rvals(1))
      external        nf_put_vara_real

      integer         nf_get_vara_real
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          real                rvals(1))
      external        nf_get_vara_real

      integer         nf_put_vara_double
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          doubleprecision     dvals(1))
      external        nf_put_vara_double

      integer         nf_get_vara_double
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          doubleprecision     dvals(1))
      external        nf_get_vara_double

!
! strided variable put/get routines:
!

      integer         nf_put_vars_text
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          character(*)        text)
      external        nf_put_vars_text

      integer         nf_get_vars_text
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          character(*)        text)
      external        nf_get_vars_text

      integer         nf_put_vars_int1
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          nf_int1_t           i1vals(1))
      external        nf_put_vars_int1

      integer         nf_get_vars_int1
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          nf_int1_t           i1vals(1))
      external        nf_get_vars_int1

      integer         nf_put_vars_int2
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          nf_int2_t           i2vals(1))
      external        nf_put_vars_int2

      integer         nf_get_vars_int2
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          nf_int2_t           i2vals(1))
      external        nf_get_vars_int2

      integer         nf_put_vars_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             ivals(1))
      external        nf_put_vars_int

      integer         nf_get_vars_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             ivals(1))
      external        nf_get_vars_int

      integer         nf_put_vars_real
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          real                rvals(1))
      external        nf_put_vars_real

      integer         nf_get_vars_real
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          real                rvals(1))
      external        nf_get_vars_real

      integer         nf_put_vars_double
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          doubleprecision     dvals(1))
      external        nf_put_vars_double

      integer         nf_get_vars_double
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          doubleprecision     dvals(1))
      external        nf_get_vars_double

!
! mapped variable put/get routines:
!

      integer         nf_put_varm_text
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          character(*)        text)
      external        nf_put_varm_text

      integer         nf_get_varm_text
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          character(*)        text)
      external        nf_get_varm_text

      integer         nf_put_varm_int1
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          nf_int1_t           i1vals(1))
      external        nf_put_varm_int1

      integer         nf_get_varm_int1
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          nf_int1_t           i1vals(1))
      external        nf_get_varm_int1

      integer         nf_put_varm_int2
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          nf_int2_t           i2vals(1))
      external        nf_put_varm_int2

      integer         nf_get_varm_int2
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          nf_int2_t           i2vals(1))
      external        nf_get_varm_int2

      integer         nf_put_varm_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          integer             ivals(1))
      external        nf_put_varm_int

      integer         nf_get_varm_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          integer             ivals(1))
      external        nf_get_varm_int

      integer         nf_put_varm_real
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          real                rvals(1))
      external        nf_put_varm_real

      integer         nf_get_varm_real
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          real                rvals(1))
      external        nf_get_varm_real

      integer         nf_put_varm_double
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          doubleprecision     dvals(1))
      external        nf_put_varm_double

      integer         nf_get_varm_double
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          doubleprecision     dvals(1))
      external        nf_get_varm_double


!     NetCDF-4.
!     This is part of netCDF-4. Copyright 2006, UCAR, See COPYRIGHT
!     file for distribution information.

!     Netcdf version 4 fortran interface.

!     $Id: netcdf4.inc,v 1.28 2010/05/25 13:53:02 ed Exp $

!     New netCDF-4 types.
      integer nf_ubyte
      integer nf_ushort
      integer nf_uint
      integer nf_int64
      integer nf_uint64
      integer nf_string
      integer nf_vlen
      integer nf_opaque
      integer nf_enum
      integer nf_compound

      parameter (nf_ubyte = 7)
      parameter (nf_ushort = 8)
      parameter (nf_uint = 9)
      parameter (nf_int64 = 10)
      parameter (nf_uint64 = 11)
      parameter (nf_string = 12)
      parameter (nf_vlen = 13)
      parameter (nf_opaque = 14)
      parameter (nf_enum = 15)
      parameter (nf_compound = 16)

!     New netCDF-4 fill values.
      integer           nf_fill_ubyte
      integer           nf_fill_ushort
!      real              nf_fill_uint
!      real              nf_fill_int64
!      real              nf_fill_uint64
      parameter (nf_fill_ubyte = 255)
      parameter (nf_fill_ushort = 65535)

!     New constants.
      integer nf_format_netcdf4
      parameter (nf_format_netcdf4 = 3)

      integer nf_format_netcdf4_classic
      parameter (nf_format_netcdf4_classic = 4)

      integer nf_netcdf4
      parameter (nf_netcdf4 = 4096)

      integer nf_classic_model
      parameter (nf_classic_model = 256)

      integer nf_chunk_seq
      parameter (nf_chunk_seq = 0)
      integer nf_chunk_sub
      parameter (nf_chunk_sub = 1)
      integer nf_chunk_sizes
      parameter (nf_chunk_sizes = 2)

      integer nf_endian_native
      parameter (nf_endian_native = 0)
      integer nf_endian_little
      parameter (nf_endian_little = 1)
      integer nf_endian_big
      parameter (nf_endian_big = 2)

!     For NF_DEF_VAR_CHUNKING
      integer nf_chunked
      parameter (nf_chunked = 0)
      integer nf_contiguous
      parameter (nf_contiguous = 1)

!     For NF_DEF_VAR_FLETCHER32
      integer nf_nochecksum
      parameter (nf_nochecksum = 0)
      integer nf_fletcher32
      parameter (nf_fletcher32 = 1)

!     For NF_DEF_VAR_DEFLATE
      integer nf_noshuffle
      parameter (nf_noshuffle = 0)
      integer nf_shuffle
      parameter (nf_shuffle = 1)

!     For NF_DEF_VAR_SZIP
      integer nf_szip_ec_option_mask
      parameter (nf_szip_ec_option_mask = 4)
      integer nf_szip_nn_option_mask
      parameter (nf_szip_nn_option_mask = 32)

!     For parallel I/O.
      integer nf_mpiio      
      parameter (nf_mpiio = 8192)
      integer nf_mpiposix
      parameter (nf_mpiposix = 16384)
      integer nf_pnetcdf
      parameter (nf_pnetcdf = 32768)

!     For NF_VAR_PAR_ACCESS.
      integer nf_independent
      parameter (nf_independent = 0)
      integer nf_collective
      parameter (nf_collective = 1)

!     New error codes.
      integer nf_ehdferr        ! Error at HDF5 layer. 
      parameter (nf_ehdferr = -101)
      integer nf_ecantread      ! Can't read. 
      parameter (nf_ecantread = -102)
      integer nf_ecantwrite     ! Can't write. 
      parameter (nf_ecantwrite = -103)
      integer nf_ecantcreate    ! Can't create. 
      parameter (nf_ecantcreate = -104)
      integer nf_efilemeta      ! Problem with file metadata. 
      parameter (nf_efilemeta = -105)
      integer nf_edimmeta       ! Problem with dimension metadata. 
      parameter (nf_edimmeta = -106)
      integer nf_eattmeta       ! Problem with attribute metadata. 
      parameter (nf_eattmeta = -107)
      integer nf_evarmeta       ! Problem with variable metadata. 
      parameter (nf_evarmeta = -108)
      integer nf_enocompound    ! Not a compound type. 
      parameter (nf_enocompound = -109)
      integer nf_eattexists     ! Attribute already exists. 
      parameter (nf_eattexists = -110)
      integer nf_enotnc4        ! Attempting netcdf-4 operation on netcdf-3 file.   
      parameter (nf_enotnc4 = -111)
      integer nf_estrictnc3     ! Attempting netcdf-4 operation on strict nc3 netcdf-4 file.   
      parameter (nf_estrictnc3 = -112)
      integer nf_enotnc3        ! Attempting netcdf-3 operation on netcdf-4 file.   
      parameter (nf_enotnc3 = -113)
      integer nf_enopar         ! Parallel operation on file opened for non-parallel access.   
      parameter (nf_enopar = -114)
      integer nf_eparinit       ! Error initializing for parallel access.   
      parameter (nf_eparinit = -115)
      integer nf_ebadgrpid      ! Bad group ID.   
      parameter (nf_ebadgrpid = -116)
      integer nf_ebadtypid      ! Bad type ID.   
      parameter (nf_ebadtypid = -117)
      integer nf_etypdefined    ! Type has already been defined and may not be edited. 
      parameter (nf_etypdefined = -118)
      integer nf_ebadfield      ! Bad field ID.   
      parameter (nf_ebadfield = -119)
      integer nf_ebadclass      ! Bad class.   
      parameter (nf_ebadclass = -120)
      integer nf_emaptype       ! Mapped access for atomic types only.   
      parameter (nf_emaptype = -121)
      integer nf_elatefill      ! Attempt to define fill value when data already exists. 
      parameter (nf_elatefill = -122)
      integer nf_elatedef       ! Attempt to define var properties, like deflate, after enddef. 
      parameter (nf_elatedef = -123)
      integer nf_edimscale      ! Probem with HDF5 dimscales. 
      parameter (nf_edimscale = -124)
      integer nf_enogrp       ! No group found.
      parameter (nf_enogrp = -125)


!     New functions.

!     Parallel I/O.
      integer nf_create_par
      external nf_create_par

      integer nf_open_par
      external nf_open_par

      integer nf_var_par_access
      external nf_var_par_access

!     Functions to handle groups.
      integer nf_inq_ncid
      external nf_inq_ncid

      integer nf_inq_grps
      external nf_inq_grps

      integer nf_inq_grpname
      external nf_inq_grpname

      integer nf_inq_grpname_full
      external nf_inq_grpname_full

      integer nf_inq_grpname_len
      external nf_inq_grpname_len

      integer nf_inq_grp_parent
      external nf_inq_grp_parent

      integer nf_inq_grp_ncid
      external nf_inq_grp_ncid

      integer nf_inq_grp_full_ncid
      external nf_inq_grp_full_ncid

      integer nf_inq_varids
      external nf_inq_varids

      integer nf_inq_dimids
      external nf_inq_dimids

      integer nf_def_grp
      external nf_def_grp

!     New rename grp function

      integer nf_rename_grp
      external nf_rename_grp

!     New options for netCDF variables.
      integer nf_def_var_deflate
      external nf_def_var_deflate

      integer nf_inq_var_deflate
      external nf_inq_var_deflate

      integer nf_def_var_fletcher32
      external nf_def_var_fletcher32

      integer nf_inq_var_fletcher32
      external nf_inq_var_fletcher32

      integer nf_def_var_chunking
      external nf_def_var_chunking

      integer nf_inq_var_chunking
      external nf_inq_var_chunking

      integer nf_def_var_fill
      external nf_def_var_fill

      integer nf_inq_var_fill
      external nf_inq_var_fill

      integer nf_def_var_endian
      external nf_def_var_endian

      integer nf_inq_var_endian
      external nf_inq_var_endian

!     User defined types.
      integer nf_inq_typeids
      external nf_inq_typeids

      integer nf_inq_typeid
      external nf_inq_typeid

      integer nf_inq_type
      external nf_inq_type

      integer nf_inq_user_type
      external nf_inq_user_type

!     User defined types - compound types.
      integer nf_def_compound
      external nf_def_compound

      integer nf_insert_compound
      external nf_insert_compound

      integer nf_insert_array_compound
      external nf_insert_array_compound

      integer nf_inq_compound
      external nf_inq_compound

      integer nf_inq_compound_name
      external nf_inq_compound_name

      integer nf_inq_compound_size
      external nf_inq_compound_size

      integer nf_inq_compound_nfields
      external nf_inq_compound_nfields

      integer nf_inq_compound_field
      external nf_inq_compound_field

      integer nf_inq_compound_fieldname
      external nf_inq_compound_fieldname

      integer nf_inq_compound_fieldindex
      external nf_inq_compound_fieldindex

      integer nf_inq_compound_fieldoffset
      external nf_inq_compound_fieldoffset

      integer nf_inq_compound_fieldtype
      external nf_inq_compound_fieldtype

      integer nf_inq_compound_fieldndims
      external nf_inq_compound_fieldndims

      integer nf_inq_compound_fielddim_sizes
      external nf_inq_compound_fielddim_sizes

!     User defined types - variable length arrays.
      integer nf_def_vlen
      external nf_def_vlen

      integer nf_inq_vlen
      external nf_inq_vlen

      integer nf_free_vlen
      external nf_free_vlen

!     User defined types - enums.
      integer nf_def_enum
      external nf_def_enum

      integer nf_insert_enum
      external nf_insert_enum

      integer nf_inq_enum
      external nf_inq_enum

      integer nf_inq_enum_member
      external nf_inq_enum_member

      integer nf_inq_enum_ident
      external nf_inq_enum_ident

!     User defined types - opaque.
      integer nf_def_opaque
      external nf_def_opaque

      integer nf_inq_opaque
      external nf_inq_opaque

!     Write and read attributes of any type, including user defined
!     types.
      integer nf_put_att
      external nf_put_att
      integer nf_get_att
      external nf_get_att

!     Write and read variables of any type, including user defined
!     types.
      integer nf_put_var
      external nf_put_var
      integer nf_put_var1
      external nf_put_var1
      integer nf_put_vara
      external nf_put_vara
      integer nf_put_vars
      external nf_put_vars
      integer nf_get_var
      external nf_get_var
      integer nf_get_var1
      external nf_get_var1
      integer nf_get_vara
      external nf_get_vara
      integer nf_get_vars
      external nf_get_vars

!     64-bit int functions.
      integer nf_put_var1_int64
      external nf_put_var1_int64
      integer nf_put_vara_int64
      external nf_put_vara_int64
      integer nf_put_vars_int64
      external nf_put_vars_int64
      integer nf_put_varm_int64
      external nf_put_varm_int64
      integer nf_put_var_int64
      external nf_put_var_int64
      integer nf_get_var1_int64
      external nf_get_var1_int64
      integer nf_get_vara_int64
      external nf_get_vara_int64
      integer nf_get_vars_int64
      external nf_get_vars_int64
      integer nf_get_varm_int64
      external nf_get_varm_int64
      integer nf_get_var_int64
      external nf_get_var_int64

!     For helping F77 users with VLENs.
      integer nf_get_vlen_element
      external nf_get_vlen_element
      integer nf_put_vlen_element
      external nf_put_vlen_element

!     For dealing with file level chunk cache.
      integer nf_set_chunk_cache
      external nf_set_chunk_cache
      integer nf_get_chunk_cache
      external nf_get_chunk_cache

!     For dealing with per variable chunk cache.
      integer nf_set_var_chunk_cache
      external nf_set_var_chunk_cache
      integer nf_get_var_chunk_cache
      external nf_get_var_chunk_cache

!     NetCDF-2.
!ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
! begin netcdf 2.4 backward compatibility:
!

!      
! functions in the fortran interface
!
      integer nccre
      integer ncopn
      integer ncddef
      integer ncdid
      integer ncvdef
      integer ncvid
      integer nctlen
      integer ncsfil

      external nccre
      external ncopn
      external ncddef
      external ncdid
      external ncvdef
      external ncvid
      external nctlen
      external ncsfil


      integer ncrdwr
      integer nccreat
      integer ncexcl
      integer ncindef
      integer ncnsync
      integer nchsync
      integer ncndirty
      integer nchdirty
      integer nclink
      integer ncnowrit
      integer ncwrite
      integer ncclob
      integer ncnoclob
      integer ncglobal
      integer ncfill
      integer ncnofill
      integer maxncop
      integer maxncdim
      integer maxncatt
      integer maxncvar
      integer maxncnam
      integer maxvdims
      integer ncnoerr
      integer ncebadid
      integer ncenfile
      integer nceexist
      integer nceinval
      integer nceperm
      integer ncenotin
      integer nceindef
      integer ncecoord
      integer ncemaxds
      integer ncename
      integer ncenoatt
      integer ncemaxat
      integer ncebadty
      integer ncebadd
      integer ncests
      integer nceunlim
      integer ncemaxvs
      integer ncenotvr
      integer nceglob
      integer ncenotnc
      integer ncfoobar
      integer ncsyserr
      integer ncfatal
      integer ncverbos
      integer ncentool


!
! netcdf data types:
!
      integer ncbyte
      integer ncchar
      integer ncshort
      integer nclong
      integer ncfloat
      integer ncdouble

      parameter(ncbyte = 1)
      parameter(ncchar = 2)
      parameter(ncshort = 3)
      parameter(nclong = 4)
      parameter(ncfloat = 5)
      parameter(ncdouble = 6)

!     
!     masks for the struct nc flag field; passed in as 'mode' arg to
!     nccreate and ncopen.
!     

!     read/write, 0 => readonly 
      parameter(ncrdwr = 1)
!     in create phase, cleared by ncendef 
      parameter(nccreat = 2)
!     on create destroy existing file 
      parameter(ncexcl = 4)
!     in define mode, cleared by ncendef 
      parameter(ncindef = 8)
!     synchronise numrecs on change (x'10')
      parameter(ncnsync = 16)
!     synchronise whole header on change (x'20')
      parameter(nchsync = 32)
!     numrecs has changed (x'40')
      parameter(ncndirty = 64)  
!     header info has changed (x'80')
      parameter(nchdirty = 128)
!     prefill vars on endef and increase of record, the default behavior
      parameter(ncfill = 0)
!     do not fill vars on endef and increase of record (x'100')
      parameter(ncnofill = 256)
!     isa link (x'8000')
      parameter(nclink = 32768)

!     
!     'mode' arguments for nccreate and ncopen
!     
      parameter(ncnowrit = 0)
      parameter(ncwrite = ncrdwr)
      parameter(ncclob = nf_clobber)
      parameter(ncnoclob = nf_noclobber)

!     
!     'size' argument to ncdimdef for an unlimited dimension
!     
      integer ncunlim
      parameter(ncunlim = 0)

!     
!     attribute id to put/get a global attribute
!     
      parameter(ncglobal  = 0)

!     
!     advisory maximums:
!     
      parameter(maxncop = 64)
      parameter(maxncdim = 1024)
      parameter(maxncatt = 8192)
      parameter(maxncvar = 8192)
!     not enforced 
      parameter(maxncnam = 256)
      parameter(maxvdims = maxncdim)

!     
!     global netcdf error status variable
!     initialized in error.c
!     

!     no error 
      parameter(ncnoerr = nf_noerr)
!     not a netcdf id 
      parameter(ncebadid = nf_ebadid)
!     too many netcdfs open 
      parameter(ncenfile = -31)   ! nc_syserr
!     netcdf file exists && ncnoclob
      parameter(nceexist = nf_eexist)
!     invalid argument 
      parameter(nceinval = nf_einval)
!     write to read only 
      parameter(nceperm = nf_eperm)
!     operation not allowed in data mode 
      parameter(ncenotin = nf_enotindefine )   
!     operation not allowed in define mode 
      parameter(nceindef = nf_eindefine)   
!     coordinates out of domain 
      parameter(ncecoord = nf_einvalcoords)
!     maxncdims exceeded 
      parameter(ncemaxds = nf_emaxdims)
!     string match to name in use 
      parameter(ncename = nf_enameinuse)   
!     attribute not found 
      parameter(ncenoatt = nf_enotatt)
!     maxncattrs exceeded 
      parameter(ncemaxat = nf_emaxatts)
!     not a netcdf data type 
      parameter(ncebadty = nf_ebadtype)
!     invalid dimension id 
      parameter(ncebadd = nf_ebaddim)
!     ncunlimited in the wrong index 
      parameter(nceunlim = nf_eunlimpos)
!     maxncvars exceeded 
      parameter(ncemaxvs = nf_emaxvars)
!     variable not found 
      parameter(ncenotvr = nf_enotvar)
!     action prohibited on ncglobal varid 
      parameter(nceglob = nf_eglobal)
!     not a netcdf file 
      parameter(ncenotnc = nf_enotnc)
      parameter(ncests = nf_ests)
      parameter (ncentool = nf_emaxname) 
      parameter(ncfoobar = 32)
      parameter(ncsyserr = -31)

!     
!     global options variable. used to determine behavior of error handler.
!     initialized in lerror.c
!     
      parameter(ncfatal = 1)
      parameter(ncverbos = 2)

!
!     default fill values.  these must be the same as in the c interface.
!
      integer filbyte
      integer filchar
      integer filshort
      integer fillong
      real filfloat
      doubleprecision fildoub

      parameter (filbyte = -127)
      parameter (filchar = 0)
      parameter (filshort = -32767)
      parameter (fillong = -2147483647)
      parameter (filfloat = 9.9692099683868690e+36)
      parameter (fildoub = 9.9692099683868690e+36)

  real :: timeOr = 0
  real :: timeSr = 0
  real :: timeCr = 0
  real :: timeGW = 0
  integer :: clock_count_1 = 0
  integer :: clock_count_2 = 0
  integer :: clock_rate    = 0
  integer :: rtout_factor

  integer, parameter :: r4 = selected_real_kind(4)
  real,    parameter :: zeroFlt=0.0000000000000000000_r4
  integer, parameter :: r8 = selected_real_kind(8)
  real*8,  parameter :: zeroDbl=0.0000000000000000000_r8

   contains
   subroutine HYDRO_rst_out(did)
      use module_stream_nudging, only: output_nudging_last_obs
      implicit none
      integer:: rst_out  
      integer did, outflag
      character(len=19) out_date
      character(len=19) str_tmp
      rst_out = -99
   if(IO_id .eq. my_id) then
     if(nlst_rt(did)%dt .gt. nlst_rt(did)%rst_dt*60) then
        call geth_newdate(out_date, nlst_rt(did)%startdate, nint(nlst_rt(did)%dt*rt_domain(did)%rst_counts))
     else
        call geth_newdate(out_date, nlst_rt(did)%startdate, nint(nlst_rt(did)%rst_dt*60*rt_domain(did)%rst_counts))
     endif
     if ( (nlst_rt(did)%rst_dt .gt. 0) .and. (out_date(1:19) == nlst_rt(did)%olddate(1:19)) ) then
           rst_out = 99
           rt_domain(did)%rst_counts = rt_domain(did)%rst_counts + 1
     endif
! restart every month automatically.
     if ( (nlst_rt(did)%olddate(9:10) == "01") .and. (nlst_rt(did)%olddate(12:13) == "00") .and. &
          (nlst_rt(did)%olddate(15:16) == "00").and. (nlst_rt(did)%olddate(18:19) == "00") .and. &
          (nlst_rt(did)%rst_dt .le. 0)  ) then 
           if(nlst_rt(did)%startdate(1:16) .ne. nlst_rt(did)%olddate(1:16) ) then
               rst_out = 99
           endif
     endif

   endif
     call mpp_land_bcast_int1(rst_out)
    if(rst_out .gt. 0) then 
      write(6,*) "yw check output restart at ",nlst_rt(did)%olddate(1:16)
      if(nlst_rt(did)%rst_bi_out .eq. 1) then
             if(my_id .lt. 10) then
                  write(str_tmp,'(I1)') my_id 
             else if(my_id .lt. 100) then 
                  write(str_tmp,'(I2)') my_id 
             else if(my_id .lt. 1000) then
                  write(str_tmp,'(I3)') my_id 
             else if(my_id .lt. 10000) then
                  write(str_tmp,'(I4)') my_id 
             else if(my_id .lt. 100000) then 
                  write(str_tmp,'(I5)') my_id 
             else
                continue
             endif
             call mpp_land_bcast_char(16,nlst_rt(did)%olddate(1:16))
             call   RESTART_OUT_bi(trim("HYDRO_RST."//nlst_rt(did)%olddate(1:16)   &
                 //"_DOMAIN"//trim(nlst_rt(did)%hgrid)//"."//trim(str_tmp)),  did)
      else
             call   RESTART_OUT_nc(trim("HYDRO_RST."//nlst_rt(did)%olddate(1:16)   &
                 //"_DOMAIN"//trim(nlst_rt(did)%hgrid)),  did)
      endif

      call output_nudging_last_obs
   endif


   end subroutine HYDRO_rst_out

   subroutine HYDRO_out(did, rstflag)
   
      implicit none
      integer did, outflag, rtflag, iniflag
      integer rstflag
      character(len=19) out_date
      integer :: Kt, ounit, i
      real, dimension(RT_DOMAIN(did)%NLINKS,2) :: str_out
      real, dimension(RT_DOMAIN(did)%NLINKS) :: vel_out 

!    real, dimension(RT_DOMAIN(did)%ix,RT_DOMAIN(did)%jx):: soilmx_tmp, &
!           runoff1x_tmp, runoff2x_tmp, runoff3x_tmp,etax_tmp, &
!           EDIRX_tmp,ECX_tmp,ETTX_tmp,RCX_tmp,HX_tmp,acrain_tmp, &
!           ACSNOM_tmp, esnow2d_tmp, drip2d_tmp,dewfall_tmp, fpar_tmp, &
!           qfx_tmp, prcp_out_tmp, etpndx_tmp

   outflag = -99

   if(IO_id .eq. my_id) then
      if(nlst_rt(did)%olddate(1:19) .eq. nlst_rt(did)%startdate(1:19) .and. rt_domain(did)%his_out_counts .eq. 0) then
         write(6,*) "output hydrology at time : ",nlst_rt(did)%olddate(1:19), rt_domain(did)%his_out_counts
         outflag = 99
      else
         if(nlst_rt(did)%dt .gt. nlst_rt(did)%out_dt*60) then
             call geth_newdate(out_date, nlst_rt(did)%startdate, nint(nlst_rt(did)%dt*rt_domain(did)%out_counts))
         else
             call geth_newdate(out_date, nlst_rt(did)%startdate, nint(nlst_rt(did)%out_dt*60*rt_domain(did)%out_counts))
         endif
         if ( out_date(1:19) == nlst_rt(did)%olddate(1:19) ) then
             write(6,*) "output hydrology at time : ",nlst_rt(did)%olddate(1:19)
             outflag = 99
         endif
      endif
   endif
     call mpp_land_bcast_int1(outflag)

     if (rstflag .eq. 1) call HYDRO_rst_out(did) 

     if (outflag .lt. 0) return

     rt_domain(did)%out_counts = rt_domain(did)%out_counts + 1

     rt_domain(did)%his_out_counts = rt_domain(did)%his_out_counts + 1

     if(nlst_rt(did)%out_dt*60 .gt. nlst_rt(did)%DT) then
        kt = rt_domain(did)%his_out_counts*nlst_rt(did)%out_dt*60/nlst_rt(did)%DT
     else
        kt = rt_domain(did)%his_out_counts
     endif

! jump the ouput for the initial time when it has restart file from routing.
   rtflag = -99
   iniflag = -99
   if(IO_id .eq. my_id) then
!       if ( (trim(nlst_rt(did)%restart_file) /= "") .and. ( nlst_rt(did)%startdate(1:19) == nlst_rt(did)%olddate(1:19) ) ) then
!#ifndef NCEP_WCOSS
!             print*, "yyyywww restart_file = ", trim(nlst_rt(did)%restart_file) 
!#else
!             write(78,*) "yyyywww restart_file = ", trim(nlst_rt(did)%restart_file) 
!#endif
          if ( nlst_rt(did)%startdate(1:19) == nlst_rt(did)%olddate(1:19) ) iniflag = 1
          if ( (trim(nlst_rt(did)%restart_file) /= "") .and. ( nlst_rt(did)%startdate(1:19) == nlst_rt(did)%olddate(1:19) ) ) rtflag = 1
!       endif
   endif  
   call mpp_land_bcast_int1(rtflag)
   call mpp_land_bcast_int1(iniflag)


!yw keep the initial time otuput for debug
      if(rtflag == 1) then 
          rt_domain(did)%restQSTRM = .false.   !!! do not reset QSTRM.. at initial time.
          if(nlst_rt(did)%t0OutputFlag .eq. 0) return
      endif

      if (iniflag == 1) then
         if(nlst_rt(did)%t0OutputFlag .eq. 0) return
      endif

     if(nlst_rt(did)%LSMOUT_DOMAIN .eq. 1)  then
        if(nlst_rt(did)%io_form_outputs .eq. 0) then
           call output_lsm(trim(nlst_rt(did)%olddate(1:4)//nlst_rt(did)%olddate(6:7)//nlst_rt(did)%olddate(9:10)  &
                       //nlst_rt(did)%olddate(12:13)//nlst_rt(did)%olddate(15:16)//  &
                       ".LSMOUT_DOMAIN"//trim(nlst_rt(did)%hgrid)),     &
                       did)
           else
           call output_lsmOut_NWM(did)
        endif
     endif

    
if(nlst_rt(did)%SUBRTSWCRT         .gt. 0 .or. &
   nlst_rt(did)%OVRTSWCRT          .gt. 0 .or. &
   nlst_rt(did)%GWBASESWCRT        .gt. 0 .or. &
   nlst_rt(did)%channel_only       .gt. 0 .or. & 
   nlst_rt(did)%channelBucket_only .gt. 0      ) then
          

   if(nlst_rt(did)%RTOUT_DOMAIN       .eq. 1 .and. &
      nlst_rt(did)%channel_only       .eq. 0 .and. &
      nlst_rt(did)%channelBucket_only .eq. 0       ) then 
              if(mod(rtout_factor,3) .eq. 0 .or. nlst_rt(did)%io_config_outputs .ne. 5) then
                 ! Output gridded routing variables on National Water Model
                 ! high-res routing grid 
                 if(nlst_rt(did)%io_form_outputs .ne. 0) then
                    call output_rt_NWM(did,nlst_rt(did)%igrid)
                 else
                    call output_rt(    &
                      nlst_rt(did)%igrid, nlst_rt(did)%split_output_count, &
                      RT_DOMAIN(did)%ixrt, RT_DOMAIN(did)%jxrt, &
                      nlst_rt(did)%nsoil, &
   !                  nlst_rt(did)%startdate, nlst_rt(did)%olddate,
   !                  RT_DOMAIN(did)%QSUBRT,&
                      nlst_rt(did)%sincedate, nlst_rt(did)%olddate, RT_DOMAIN(did)%QSUBRT,&
                      RT_DOMAIN(did)%ZWATTABLRT,RT_DOMAIN(did)%SMCRT,&
                      RT_DOMAIN(did)%SUB_RESID,       &
                      RT_DOMAIN(did)%q_sfcflx_x,RT_DOMAIN(did)%q_sfcflx_y,&
                      RT_DOMAIN(did)%soxrt,RT_DOMAIN(did)%soyrt,&
                      RT_DOMAIN(did)%QSTRMVOLRT_ACC,RT_DOMAIN(did)%SFCHEADSUBRT, &
                      nlst_rt(did)%geo_finegrid_flnm,nlst_rt(did)%DT,&
                      RT_DOMAIN(did)%SLDPTH,RT_DOMAIN(did)%LATVAL,&
                      RT_DOMAIN(did)%LONVAL,RT_DOMAIN(did)%dist,nlst_rt(did)%RTOUT_DOMAIN,&
                      RT_DOMAIN(did)%QBDRYRT, &
                      nlst_rt(did)%io_config_outputs &
                       )
                 endif ! End check for io_form_outputs value
              endif ! End check for rtout_factor
           rtout_factor = rtout_factor + 1
   endif
   !! JLM disable GW output for NWM. Bring this line back when runtime output options avail.
   !! JLM This seems like a more logical place?
   if(nlst_rt(did)%io_form_outputs .ne. 0) then
      if(nlst_rt(did)%GWBASESWCRT .ne. 0) then
         if(nlst_rt(did)%channel_only .eq. 0) then
            if(nlst_rt(did)%output_gw .ne. 0) then
               call output_gw_NWM(did,nlst_rt(did)%igrid)
            endif
         endif
      endif
   else
      if(nlst_rt(did)%GWBASESWCRT .eq. 1) then 
         if(nlst_rt(did)%channel_only .eq. 0) then
            if(nlst_rt(did)%output_gw  .eq. 1) call output_GW_Diag(did)
         endif
      end if
   endif


   if(nlst_rt(did)%GWBASESWCRT .eq. 3) then
	     
              if(nlst_rt(did)%output_gw  .eq. 1)  &
              call sub_output_gw(    &
                nlst_rt(did)%igrid, nlst_rt(did)%split_output_count, &
                RT_DOMAIN(did)%ixrt, RT_DOMAIN(did)%jxrt,          &
                nlst_rt(did)%nsoil,                               &
!               nlst_rt(did)%startdate, nlst_rt(did)%olddate,    &
                nlst_rt(did)%sincedate, nlst_rt(did)%olddate,    &
                gw2d(did)%h, RT_DOMAIN(did)%SMCRT,                   &
                gw2d(did)%convgw, gw2d(did)%excess,                  &
                gw2d(did)%qsgwrt, gw2d(did)%qgw_chanrt,              &
                nlst_rt(did)%geo_finegrid_flnm,nlst_rt(did)%DT, &
                RT_DOMAIN(did)%SLDPTH,RT_DOMAIN(did)%LATVAL,       &
                RT_DOMAIN(did)%LONVAL,rt_domain(did)%dist,           &
                nlst_rt(did)%output_gw)

	  endif
! BF end gw2d output section

          write(6,*) "before call output_chrt"
          call flush(6)
     
           if (nlst_rt(did)%CHANRTSWCRT.eq.1.or.nlst_rt(did)%CHANRTSWCRT.eq.2) then 

!ADCHANGE: Change values for within lake reaches to NA
             str_out = RT_DOMAIN(did)%QLINK
             vel_out = RT_DOMAIN(did)%velocity

             if (RT_DOMAIN(did)%NLAKES .gt. 0)  then
                    do i=1,RT_DOMAIN(did)%NLINKS
                        if (RT_DOMAIN(did)%TYPEL(i) .eq. 2) then
                            str_out(i,1) = -9.E15 
                            vel_out(i) = -9.E15
                        endif
                    end do
             endif
!ADCHANGE: End

   if(nlst_rt(did)%io_form_outputs .ne. 0) then
      ! Call National Water Model output routine for output on NHD forecast points.
      if(nlst_rt(did)%CHRTOUT_DOMAIN .ne. 0) then
         call output_chrt_NWM(did)
      endif
      ! Call the subroutine to output frxstPts.
      if(nlst_rt(did)%frxst_pts_out .ne. 0) then
         call output_frxstPts(did)
      endif
      ! Call the subroutine to output CHANOBS
      if(nlst_rt(did)%CHANOBS_DOMAIN .ne. 0) then
         call output_chanObs_NWM(did)
      endif
   else
      ! Call traditional output routines
             if(nlst_rt(did)%CHRTOUT_DOMAIN  .eq. 1)  then
                 call mpp_output_chrt( &
                      rt_domain(did)%gnlinks,rt_domain(did)%gnlinksl,rt_domain(did)%map_l2g, &
                   nlst_rt(did)%igrid, nlst_rt(did)%split_output_count, &
                   RT_DOMAIN(did)%NLINKS,RT_DOMAIN(did)%ORDER, &
                   nlst_rt(did)%sincedate,nlst_rt(did)%olddate,RT_DOMAIN(did)%CHLON,&
                   RT_DOMAIN(did)%CHLAT,                                      &
                   RT_DOMAIN(did)%HLINK, RT_DOMAIN(did)%ZELEV,                &
                   !RT_DOMAIN(did)%QLINK,nlst_rt(did)%DT,Kt,                   &
                   str_out, nlst_rt(did)%DT,Kt,                   &
                   RT_DOMAIN(did)%STRMFRXSTPTS,nlst_rt(did)%order_to_write,   &
                   RT_DOMAIN(did)%NLINKSL,nlst_rt(did)%channel_option,        &
                   rt_domain(did)%gages, rt_domain(did)%gageMiss,             &
                   nlst_rt(did)%dt                                            &
                   , RT_DOMAIN(did)%nudge                                     &
                   , RT_DOMAIN(did)%accSfcLatRunoff, RT_DOMAIN(did)%accBucket &
                   , RT_DOMAIN(did)%qSfcLatRunoff,   RT_DOMAIN(did)%qBucket   &
                   , RT_DOMAIN(did)%qin_gwsubbas                              &
                   , nlst_rt(did)%UDMP_OPT                                    &
                   )
              else
                if(nlst_rt(did)%CHRTOUT_DOMAIN  .eq. 2)  then
                     call mpp_output_chrt2(&
                          rt_domain(did)%gnlinks,rt_domain(did)%gnlinksl,rt_domain(did)%map_l2g, &
                     nlst_rt(did)%igrid, nlst_rt(did)%split_output_count, &
                     RT_DOMAIN(did)%NLINKS,RT_DOMAIN(did)%ORDER,          &
                     nlst_rt(did)%sincedate,nlst_rt(did)%olddate,         &
                     RT_DOMAIN(did)%CHLON, RT_DOMAIN(did)%CHLAT,          &
                     RT_DOMAIN(did)%HLINK, RT_DOMAIN(did)%ZELEV,          &
                     !RT_DOMAIN(did)%QLINK,nlst_rt(did)%DT,Kt,             &
                     str_out, nlst_rt(did)%DT,Kt,                         &
                     RT_DOMAIN(did)%NLINKSL,nlst_rt(did)%channel_option,  &
                     rt_domain(did)%linkid                                &
                     , RT_DOMAIN(did)%nudge &
                     !, RT_DOMAIN(did)%QLateral, nlst_rt(did)%io_config_outputs,
                     !RT_DOMAIN(did)%velocity &
                     , RT_DOMAIN(did)%QLateral, nlst_rt(did)%io_config_outputs, vel_out &
                     , RT_DOMAIN(did)%accSfcLatRunoff, RT_DOMAIN(did)%accBucket &
                     , RT_DOMAIN(did)%qSfcLatRunoff,   RT_DOMAIN(did)%qBucket &
                     , RT_DOMAIN(did)%qin_gwsubbas,    nlst_rt(did)%UDMP_OPT &
                     )
                 endif

              endif
   endif ! End of checking for io_form_outputs flag value      
      
              if(nlst_rt(did)%CHRTOUT_GRID  .eq. 1)  then
                 if(nlst_rt(did)%io_form_outputs .eq. 0) then
                    call mpp_output_chrtgrd(nlst_rt(did)%igrid, nlst_rt(did)%split_output_count, &
                    RT_DOMAIN(did)%ixrt,RT_DOMAIN(did)%jxrt, RT_DOMAIN(did)%NLINKS,   &
                    RT_DOMAIN(did)%GCH_NETLNK, &
                    nlst_rt(did)%startdate, nlst_rt(did)%olddate, &
                    !RT_DOMAIN(did)%qlink, nlst_rt(did)%dt, nlst_rt(did)%geo_finegrid_flnm,   &
                    str_out, nlst_rt(did)%dt, nlst_rt(did)%geo_finegrid_flnm,   &
                    RT_DOMAIN(did)%gnlinks,RT_DOMAIN(did)%map_l2g,                   &
                    RT_DOMAIN(did)%g_ixrt,RT_DOMAIN(did)%g_jxrt )
                 else
                    call output_chrtout_grd_NWM(did,nlst_rt(did)%igrid)
                 endif
              endif
               if (RT_DOMAIN(did)%NLAKES.gt.0)  then
                      if(nlst_rt(did)%io_form_outputs .ne. 0) then
                         ! Output lakes in NWM format
                         if(nlst_rt(did)%outlake .ne. 0) then
                            call output_lakes_NWM(did,nlst_rt(did)%igrid)
                         endif
                      else
                         if(nlst_rt(did)%outlake .eq. 1) then
                             call mpp_output_lakes( RT_DOMAIN(did)%lake_index, &
                                  nlst_rt(did)%igrid, nlst_rt(did)%split_output_count, &
                                  RT_DOMAIN(did)%NLAKES, &
                                  trim(nlst_rt(did)%sincedate), trim(nlst_rt(did)%olddate), &
                                  RT_DOMAIN(did)%LATLAKE,RT_DOMAIN(did)%LONLAKE, &
                                  RT_DOMAIN(did)%ELEVLAKE,RT_DOMAIN(did)%QLAKEI, &
                                  RT_DOMAIN(did)%QLAKEO, &
                                  RT_DOMAIN(did)%RESHT,nlst_rt(did)%DT,Kt)
                         endif
                         if(nlst_rt(did)%outlake .eq. 2) then
                             call mpp_output_lakes2( RT_DOMAIN(did)%lake_index, &
                                  nlst_rt(did)%igrid, nlst_rt(did)%split_output_count, &
                                  RT_DOMAIN(did)%NLAKES, &
                                  trim(nlst_rt(did)%sincedate), trim(nlst_rt(did)%olddate), &
                                  RT_DOMAIN(did)%LATLAKE,RT_DOMAIN(did)%LONLAKE, &
                                  RT_DOMAIN(did)%ELEVLAKE,RT_DOMAIN(did)%QLAKEI, &
                                  RT_DOMAIN(did)%QLAKEO, &
                                  RT_DOMAIN(did)%RESHT,nlst_rt(did)%DT,Kt,RT_DOMAIN(did)%LAKEIDM)
                         endif

                      endif ! end of check for io_form_outputs value
               endif   ! end if block of rNLAKES .gt. 0
        endif 
           write(6,*) "end calling output functions"

        endif  ! end of routing switch


      end subroutine HYDRO_out


      subroutine HYDRO_rst_in(did)
        integer :: did
        integer:: flag 



   flag = -1
   if(my_id.eq.IO_id) then
      if (trim(nlst_rt(did)%restart_file) /= "") then
          flag = 99
          rt_domain(did)%timestep_flag = 99   ! continue run
      endif 
   endif 
   call mpp_land_bcast_int1(flag)

   nlst_rt(did)%sincedate = nlst_rt(did)%startdate
   
   if (flag.eq.99) then

     if(my_id.eq.IO_id) then
        write(6,*) "*** read restart data: ",trim(nlst_rt(did)%restart_file)
     endif 

      if(nlst_rt(did)%rst_bi_in .eq. 1) then
         call RESTART_IN_bi(trim(nlst_rt(did)%restart_file), did)
      else
         call RESTART_IN_nc(trim(nlst_rt(did)%restart_file), did)
      endif

!yw  if (trim(nlst_rt(did)%restart_file) /= "") then 
!yw          nlst_rt(did)%restart_file = ""
!yw  endif

  endif
 end subroutine HYDRO_rst_in

     subroutine HYDRO_time_adv(did)
        implicit none
        character(len = 19) :: newdate 
        integer did
 
   if(IO_id.eq.my_id) then
         call geth_newdate(newdate, nlst_rt(did)%olddate, nint( nlst_rt(did)%dt))
         nlst_rt(did)%olddate = newdate
         write(6,*) "current time is ",newdate
   endif
     end subroutine HYDRO_time_adv
  
     subroutine HYDRO_exe(did)


        implicit none
        integer:: did
        integer:: rst_out


!       call HYDRO_out(did)


! running land surface model
! cpl: 0--offline run; 
!      1-- coupling with WRF but running offline lsm; 
!      2-- coupling with WRF but do not run offline lsm  
!      3-- coupling with LIS and do not run offline lsm  
!      4:  coupling with CLM
!          if(nlst_rt(did)%SYS_CPL .eq. 0 .or. nlst_rt(did)%SYS_CPL .eq. 1 )then
!                  call drive_noahLSF(did,kt)
!          else
!              ! does not run the NOAH LASF model, only read the parameter
!              call read_land_par(did,lsm(did)%ix,lsm(did)%jx)
!          endif





if (nlst_rt(did)%GWBASESWCRT        .ne. 0 .or. &
    nlst_rt(did)%SUBRTSWCRT         .ne. 0 .or. &
    nlst_rt(did)%OVRTSWCRT          .ne. 0 .or. &
    nlst_rt(did)%channel_only       .ne. 0 .or. &
    nlst_rt(did)%channelBucket_only .ne. 0      ) then

   ! step 1) disaggregate specific fields from LSM to Hydro grid
   if(nlst_rt(did)%channel_only .eq. 0 .and. nlst_rt(did)%channelBucket_only .eq. 0) then

      RT_DOMAIN(did)%QSTRMVOLRT = zeroFlt
      RT_DOMAIN(did)%LAKE_INFLORT_DUM = RT_DOMAIN(did)%LAKE_INFLORT

      if(nlst_rt(did)%SUBRTSWCRT .ne. 0 .or. nlst_rt(did)%OVRTSWCRT .ne. 0) then
         call disaggregateDomain_drv(did)
      endif
      if(nlst_rt(did)%OVRTSWCRT .eq. 0) then
         if(nlst_rt(did)%UDMP_OPT .eq. 1) then
            call RunOffDisag(RT_DOMAIN(did)%INFXSRT, RT_DOMAIN(did)%landRunOff,        &
                 rt_domain(did)%dist_lsm(:,:,9),RT_DOMAIN(did)%dist(:,:,9), &
                 RT_DOMAIN(did)%INFXSWGT, nlst_rt(did)%AGGFACTRT, RT_DOMAIN(did)%ix,RT_DOMAIN(did)%jx)
         endif
      endif

      call system_clock(count=clock_count_1, count_rate=clock_rate)
   endif !! channel_only & channelBucket_only == 0


   ! step 2)
   if(nlst_rt(did)%channel_only .eq. 0 .and. nlst_rt(did)%channelBucket_only .eq. 0) then
      if(nlst_rt(did)%SUBRTSWCRT .ne.0) then
         call SubsurfaceRouting_drv(did)
      endif
      call system_clock(count=clock_count_2, count_rate=clock_rate)
      timeSr = timeSr     + float(clock_count_2-clock_count_1)/float(clock_rate)
      write(6,*) "Timing: Subsurface Routing  accumulated time--", timeSr
   end if !! channel_only & channelBucket_only == 0				


   ! step 3) todo split
   if(nlst_rt(did)%channel_only .eq. 0 .and. nlst_rt(did)%channelBucket_only .eq. 0) then
      call system_clock(count=clock_count_1, count_rate=clock_rate)
      if(nlst_rt(did)%OVRTSWCRT .ne. 0) then
         call OverlandRouting_drv(did)
      else
         RT_DOMAIN(did)%SFCHEADSUBRT = RT_DOMAIN(did)%INFXSUBRT
         RT_DOMAIN(did)%INFXSUBRT = 0.
      endif
      call system_clock(count=clock_count_2, count_rate=clock_rate)
      timeOr = timeOr     + float(clock_count_2-clock_count_1)/float(clock_rate)
      write(6,*) "Timing: Overland Routing  accumulated time--", timeOr

      RT_DOMAIN(did)%QSTRMVOLRT_TS = RT_DOMAIN(did)%QSTRMVOLRT
      RT_DOMAIN(did)%QSTRMVOLRT_ACC = RT_DOMAIN(did)%QSTRMVOLRT_ACC + RT_DOMAIN(did)%QSTRMVOLRT_TS
      
      RT_DOMAIN(did)%LAKE_INFLORT_TS = RT_DOMAIN(did)%LAKE_INFLORT-RT_DOMAIN(did)%LAKE_INFLORT_DUM
			
      call system_clock(count=clock_count_1, count_rate=clock_rate)
   end if !! channel_only & channelBucket_only == 0


   ! step 4) baseflow or groundwater physics
   !! channelBucket_only can be anything: the only time you dont run this is if channel_only=1
   if(nlst_rt(did)%channel_only .eq. 0) then  
      if (nlst_rt(did)%GWBASESWCRT .gt. 0) then
         call driveGwBaseflow(did)
      endif
      call system_clock(count=clock_count_2, count_rate=clock_rate)
      timeGw = timeGw     + float(clock_count_2-clock_count_1)/float(clock_rate)
      write(6,*) "Timing: GwBaseflow  accumulated time--", timeGw
      call system_clock(count=clock_count_1, count_rate=clock_rate)
   end if !! channel_only == 0

   ! step 5) river channel physics
   call driveChannelRouting(did)
   call system_clock(count=clock_count_2, count_rate=clock_rate)
   timeCr = timeCr     + float(clock_count_2-clock_count_1)/float(clock_rate)
   write(6,*) "Timing: Channel Routing  accumulated time--", timeCr

   !! if not channel_only
   if(nlst_rt(did)%channel_only .eq. 0 .and. nlst_rt(did)%channelBucket_only .eq. 0) then
	
      ! step 6) aggregate specific fields from Hydro to LSM grid
      if (nlst_rt(did)%SUBRTSWCRT .ne.0  .or. nlst_rt(did)%OVRTSWCRT .ne. 0 ) then
         call aggregateDomain(did)
      endif

   end if !! channel_only & channelBucket_only == 0

end if


!yw  if (nlst_rt(did)%sys_cpl .eq. 2) then
      ! advance to next time step
!          call HYDRO_time_adv(did)
      ! output for history 
!          call HYDRO_out(did)
!yw  endif
           call HYDRO_time_adv(did)
           call HYDRO_out(did, 1)


!           write(90 + my_id,*) "finish calling hydro_exe"
!           call flush(90+my_id)
!          call mpp_land_sync()



           !! Under channel-only, these variables are not allocated
           if(allocated(RT_DOMAIN(did)%SOLDRAIN)) RT_DOMAIN(did)%SOLDRAIN = 0
           if(allocated(RT_DOMAIN(did)%QSUBRT))   RT_DOMAIN(did)%QSUBRT   = 0            



      end subroutine HYDRO_exe

      
      
!----------------------------------------------------      
subroutine driveGwBaseflow(did)
       
implicit none
integer, intent(in) :: did
       
integer :: i, jj, ii

!------------------------------------------------------------------
!DJG Begin GW/Baseflow Routines
!-------------------------------------------------------------------

if (nlst_rt(did)%GWBASESWCRT.ge.1) then     ! Switch to activate/specify GW/Baseflow

!  IF (nlst_rt(did)%GWBASESWCRT.GE.1000) THEN     ! Switch to activate/specify GW/Baseflow

   if (nlst_rt(did)%GWBASESWCRT.eq.1.or.nlst_rt(did)%GWBASESWCRT.eq.2) then   ! Call simple bucket baseflow scheme

      write(6,*) "*****yw******start simp_gw_buck "

      if(nlst_rt(did)%UDMP_OPT .eq. 1) then
         call simp_gw_buck_nhd(                                             &
              RT_DOMAIN(did)%ix,            RT_DOMAIN(did)%jx,              &
              RT_DOMAIN(did)%ixrt,          RT_DOMAIN(did)%jxrt,            &
              RT_DOMAIN(did)%numbasns,      nlst_rt(did)%AGGFACTRT,         &
              nlst_rt(did)%DT,              RT_DOMAIN(did)%INFXSWGT,        &
              RT_DOMAIN(did)%INFXSRT,       RT_DOMAIN(did)%SOLDRAIN,        &
              RT_DOMAIN(did)%dist(:,:,9),   rt_domain(did)%dist_lsm(:,:,9), &
              RT_DOMAIN(did)%gw_buck_coeff, RT_DOMAIN(did)%gw_buck_exp,     &
              RT_DOMAIN(did)%z_max,         RT_DOMAIN(did)%z_gwsubbas,      &
              RT_DOMAIN(did)%qout_gwsubbas, RT_DOMAIN(did)%qin_gwsubbas,    &
              nlst_rt(did)%GWBASESWCRT,     nlst_rt(did)%OVRTSWCRT,         & 
              RT_DOMAIN(did)%LNLINKSL,                                      &
              rt_domain(did)%basns_area,                                    &
              rt_domain(did)%nhdBuckMask,   nlst_rt(did)%channelBucket_only )

       else
          call simp_gw_buck(RT_DOMAIN(did)%ix,RT_DOMAIN(did)%jx,RT_DOMAIN(did)%ixrt,&
             RT_DOMAIN(did)%jxrt,RT_DOMAIN(did)%numbasns,RT_DOMAIN(did)%gnumbasns,& 
             RT_DOMAIN(did)%basns_area,&
             RT_DOMAIN(did)%basnsInd, RT_DOMAIN(did)%gw_strm_msk_lind,             &
             RT_DOMAIN(did)%gwsubbasmsk, RT_DOMAIN(did)%INFXSRT, &
             RT_DOMAIN(did)%SOLDRAIN, &
             RT_DOMAIN(did)%z_gwsubbas,&
             RT_DOMAIN(did)%qin_gwsubbas,RT_DOMAIN(did)%qout_gwsubbas,&
             RT_DOMAIN(did)%qinflowbase,&
             RT_DOMAIN(did)%gw_strm_msk,RT_DOMAIN(did)%gwbas_pix_ct, &
             RT_DOMAIN(did)%dist,nlst_rt(did)%DT,&
             RT_DOMAIN(did)%gw_buck_coeff,RT_DOMAIN(did)%gw_buck_exp, &
             RT_DOMAIN(did)%z_max,&
             nlst_rt(did)%GWBASESWCRT,nlst_rt(did)%OVRTSWCRT)
        endif

!! JLM: There's *perhaps* a better location for this output above.
!! If above is better, remove this when runtime output options are avail. 
!#ifndef HYDRO_REALTIME
!        call output_GW_Diag(did)
!#endif

           write(6,*) "*****yw******end simp_gw_buck "

    !!!For parameter setup runs output the percolation for each basin,
    !!!otherwise comment out this output...
    else if (nlst_rt(did)%gwBaseSwCRT .eq. 3) then

           write(6,*) "*****bf******start 2d_gw_model "

	   ! 	   compute qsgwrt  between lsm and gw with namelist selected coupling method
	   ! 	   qsgwrt is defined on the routing grid  and needs to be aggregated for SFLX
	   if (nlst_rt(did)%gwsoilcpl .GT. 0) THEN
	     
	     call gwSoilFlux(did)
	     
	   end if
           	   
	   gw2d(did)%excess = 0.
	   
           call gwstep(gw2d(did)%ix, gw2d(did)%jx, gw2d(did)%dx, &
			gw2d(did)%ltype, gw2d(did)%elev, gw2d(did)%bot, &
			gw2d(did)%hycond, gw2d(did)%poros, gw2d(did)%compres, &
			gw2d(did)%ho, gw2d(did)%h, gw2d(did)%convgw, &
			gw2d(did)%excess, &
			gw2d(did)%ebot, gw2d(did)%eocn, gw2d(did)%dt, &
			gw2d(did)%istep)
                      
	  gw2d(did)%ho = gw2d(did)%h
 
	  ! put surface exceeding groundwater to surface routing inflow
          RT_DOMAIN(did)%SFCHEADSUBRT = RT_DOMAIN(did)%SFCHEADSUBRT &
                                        + gw2d(did)%excess*1000. ! convert to mm
	  
	  ! aggregate  qsgw from routing to lsm grid
	  call aggregateQsgw(did)

          gw2d(did)%istep =  gw2d(did)%istep + 1
	  
           write(6,*) "*****bf******end 2d_gw_model "
      
        end if

     end if    !DJG (End if for RTE SWC activation)
!------------------------------------------------------------------
!DJG End GW/Baseflow Routines
!-------------------------------------------------------------------
     end subroutine driveGwBaseflow
      
      
      
      
!-------------------------------------------      
      subroutine driveChannelRouting(did)
       
       implicit none
       integer, intent(in) :: did
       
!-------------------------------------------------------------------
!-------------------------------------------------------------------
!DJG,DNY  Begin Channel and Lake Routing Routines
!-------------------------------------------------------------------

if (nlst_rt(did)%CHANRTSWCRT.eq.1 .or. nlst_rt(did)%CHANRTSWCRT.eq.2) then

 if(nlst_rt(did)%UDMP_OPT .eq. 1) then
     !!! for user defined Reach based Routing method.

    call drive_CHANNEL_RSL(nlst_rt(did)%UDMP_OPT,RT_DOMAIN(did)%timestep_flag, RT_DOMAIN(did)%IXRT,RT_DOMAIN(did)%JXRT,  &
       RT_DOMAIN(did)%LAKE_INFLORT_TS, RT_DOMAIN(did)%QSTRMVOLRT_TS, RT_DOMAIN(did)%TO_NODE, RT_DOMAIN(did)%FROM_NODE, &
       RT_DOMAIN(did)%TYPEL, RT_DOMAIN(did)%ORDER, RT_DOMAIN(did)%MAXORDER,   RT_DOMAIN(did)%CH_LNKRT, &
       RT_DOMAIN(did)%LAKE_MSKRT, nlst_rt(did)%DT, nlst_rt(did)%DTCT, nlst_rt(did)%DTRT_CH, &
       RT_DOMAIN(did)%MUSK, RT_DOMAIN(did)%MUSX, RT_DOMAIN(did)%QLINK, &
       RT_DOMAIN(did)%CHANLEN, RT_DOMAIN(did)%MannN, RT_DOMAIN(did)%So, RT_DOMAIN(did)%ChSSlp,RT_DOMAIN(did)%Bw, &
       RT_DOMAIN(did)%RESHT, RT_DOMAIN(did)%HRZAREA, RT_DOMAIN(did)%LAKEMAXH, RT_DOMAIN(did)%WEIRH, RT_DOMAIN(did)%WEIRC, &
       RT_DOMAIN(did)%WEIRL, RT_DOMAIN(did)%ORIFICEC, RT_DOMAIN(did)%ORIFICEA, &
       RT_DOMAIN(did)%ORIFICEE,  RT_DOMAIN(did)%CVOL, RT_DOMAIN(did)%QLAKEI, &
       RT_DOMAIN(did)%QLAKEO, RT_DOMAIN(did)%LAKENODE, &
       RT_DOMAIN(did)%QINFLOWBASE, RT_DOMAIN(did)%CHANXI, RT_DOMAIN(did)%CHANYJ, nlst_rt(did)%channel_option,  &
       RT_DOMAIN(did)%nlinks, RT_DOMAIN(did)%NLINKSL, RT_DOMAIN(did)%LINKID, RT_DOMAIN(did)%node_area,         &
       RT_DOMAIN(did)%qout_gwsubbas, &
       RT_DOMAIN(did)%LAKEIDA, RT_DOMAIN(did)%LAKEIDM, RT_DOMAIN(did)%NLAKES, RT_DOMAIN(did)%LAKEIDX   &
       , RT_DOMAIN(did)%nlinks_index, RT_DOMAIN(did)%mpp_nlinks, RT_DOMAIN(did)%yw_mpp_nlinks  &
       , RT_DOMAIN(did)%LNLINKSL   &
       , RT_DOMAIN(did)%gtoNode, RT_DOMAIN(did)%toNodeInd, RT_DOMAIN(did)%nToInd      &
       , RT_DOMAIN(did)%CH_LNKRT_SL, RT_DOMAIN(did)%landRunOff   &
       , RT_DOMAIN(did)%nudge &
       , rt_domain(did)%accSfcLatRunoff,    rt_domain(did)%accBucket &
       , rt_domain(did)%qSfcLatRunoff,        rt_domain(did)%qBucket &
       , rt_domain(did)%QLateral,            rt_domain(did)%velocity &
       , rt_domain(did)%nlinksize,            nlst_rt(did)%OVRTSWCRT &
       ,                                     nlst_rt(did)%SUBRTSWCRT &
       , nlst_rt(did)%channel_only , nlst_rt(did)%channelBucket_only &
       )             

else

    call drive_CHANNEL(RT_DOMAIN(did)%latval,RT_DOMAIN(did)%lonval, &
       RT_DOMAIN(did)%timestep_flag,RT_DOMAIN(did)%IXRT,RT_DOMAIN(did)%JXRT, &
       nlst_rt(did)%SUBRTSWCRT, RT_DOMAIN(did)%QSUBRT, &
       RT_DOMAIN(did)%LAKE_INFLORT_TS, RT_DOMAIN(did)%QSTRMVOLRT_TS,&
       RT_DOMAIN(did)%TO_NODE, RT_DOMAIN(did)%FROM_NODE, RT_DOMAIN(did)%TYPEL,&
       RT_DOMAIN(did)%ORDER, RT_DOMAIN(did)%MAXORDER, RT_DOMAIN(did)%NLINKS,&
       RT_DOMAIN(did)%CH_NETLNK, RT_DOMAIN(did)%CH_NETRT,RT_DOMAIN(did)%CH_LNKRT,&
       RT_DOMAIN(did)%LAKE_MSKRT, nlst_rt(did)%DT, nlst_rt(did)%DTCT, nlst_rt(did)%DTRT_CH,&
       RT_DOMAIN(did)%MUSK, RT_DOMAIN(did)%MUSX,  RT_DOMAIN(did)%QLINK, &
       RT_DOMAIN(did)%QLateral, &
       RT_DOMAIN(did)%HLINK, RT_DOMAIN(did)%ELRT,RT_DOMAIN(did)%CHANLEN,&
       RT_DOMAIN(did)%MannN,RT_DOMAIN(did)%So, RT_DOMAIN(did)%ChSSlp, &
       RT_DOMAIN(did)%Bw,&
       RT_DOMAIN(did)%RESHT, RT_DOMAIN(did)%HRZAREA, RT_DOMAIN(did)%LAKEMAXH,&
       RT_DOMAIN(did)%WEIRH,    &
       RT_DOMAIN(did)%WEIRC, RT_DOMAIN(did)%WEIRL, RT_DOMAIN(did)%ORIFICEC, &
       RT_DOMAIN(did)%ORIFICEA, &
       RT_DOMAIN(did)%ORIFICEE, RT_DOMAIN(did)%ZELEV, RT_DOMAIN(did)%CVOL, &
       RT_DOMAIN(did)%NLAKES, RT_DOMAIN(did)%QLAKEI, RT_DOMAIN(did)%QLAKEO,&
       RT_DOMAIN(did)%LAKENODE, RT_DOMAIN(did)%dist, &
       RT_DOMAIN(did)%QINFLOWBASE, RT_DOMAIN(did)%CHANXI, &
       RT_DOMAIN(did)%CHANYJ, nlst_rt(did)%channel_option, &
       RT_DOMAIN(did)%RETDEP_CHAN, RT_DOMAIN(did)%NLINKSL, RT_DOMAIN(did)%LINKID, &
       RT_DOMAIN(did)%node_area  &
       ,RT_DOMAIN(did)%lake_index,RT_DOMAIN(did)%link_location,&
       RT_DOMAIN(did)%mpp_nlinks,RT_DOMAIN(did)%nlinks_index, &
       RT_DOMAIN(did)%yw_mpp_nlinks  &
       , RT_DOMAIN(did)%LNLINKSL,RT_DOMAIN(did)%LLINKID &
       , rt_domain(did)%gtoNode,rt_domain(did)%toNodeInd,rt_domain(did)%nToInd  &
       , rt_domain(did)%CH_LNKRT_SL   &
       ,nlst_rt(did)%GwBaseSwCRT, gw2d(did)%ho, gw2d(did)%qgw_chanrt, &
       nlst_rt(did)%gwChanCondSw, nlst_rt(did)%gwChanCondConstIn, &
       nlst_rt(did)%gwChanCondConstOut, rt_domain(did)%velocity &
       )
endif
    
    if((nlst_rt(did)%gwBaseSwCRT == 3) .and. (nlst_rt(did)%gwChanCondSw .eq. 1)) then
      
           ! add/rm channel-aquifer exchange contribution
           
           gw2d(did)%ho =  gw2d(did)%ho  &
                        +(((gw2d(did)%qgw_chanrt*(-1)) * gw2d(did)%dt / gw2d(did)%dx**2) &
                        /  gw2d(did)%poros)
      
    endif
  endif

           write(6,*) "*****yw******end drive_CHANNEL "
      
      end subroutine driveChannelRouting
 
 
 
!------------------------------------------------ 
      subroutine aggregateDomain(did)
      
        use module_mpp_land, only:  sum_real1, my_id, io_id, numprocs
 
       implicit none
       integer, intent(in) :: did

       integer :: i, j, krt, ixxrt, jyyrt, &
                  AGGFACYRT, AGGFACXRT

! ADCHANGE: Water balance variables
       integer :: kk
       real    :: smcrttot1,smctot2,sicetot2
       real    :: suminfxsrt1,suminfxs2

 	print *, "Beginning Aggregation..."

! ADCHANGE: START Initial water balance variables
! ALL VARS in MM
        suminfxsrt1 = 0.
        smcrttot1 = 0.
        do i=1,RT_DOMAIN(did)%IXRT
         do j=1,RT_DOMAIN(did)%JXRT
            suminfxsrt1 = suminfxsrt1 + RT_DOMAIN(did)%SFCHEADSUBRT(I,J) &
                               / float(RT_DOMAIN(did)%IXRT * RT_DOMAIN(did)%JXRT)
            do kk=1,nlst_rt(did)%NSOIL
                smcrttot1 = smcrttot1 + RT_DOMAIN(did)%SMCRT(I,J,KK)*RT_DOMAIN(did)%SLDPTH(KK)*1000. &
                               / float(RT_DOMAIN(did)%IXRT * RT_DOMAIN(did)%JXRT)
            end do
         end do
        end do
! not tested
        CALL sum_real1(suminfxsrt1)
        CALL sum_real1(smcrttot1)
        suminfxsrt1 = suminfxsrt1/float(numprocs)
        smcrttot1 = smcrttot1/float(numprocs)
! END Initial water balance variables

        do J=1,RT_DOMAIN(did)%JX
          do I=1,RT_DOMAIN(did)%IX

             RT_DOMAIN(did)%SFCHEADAGGRT = 0.
!DJG Subgrid weighting edit...
             RT_DOMAIN(did)%LSMVOL=0.
             do KRT=1,nlst_rt(did)%NSOIL
!                SMCAGGRT(KRT) = 0.
               RT_DOMAIN(did)%SH2OAGGRT(KRT) = 0.
             end do


             do AGGFACYRT=nlst_rt(did)%AGGFACTRT-1,0,-1
              do AGGFACXRT=nlst_rt(did)%AGGFACTRT-1,0,-1


                IXXRT=I*nlst_rt(did)%AGGFACTRT-AGGFACXRT
                JYYRT=J*nlst_rt(did)%AGGFACTRT-AGGFACYRT
       if(left_id.ge.0) IXXRT=IXXRT+1
       if(down_id.ge.0) JYYRT=JYYRT+1

!State Variables
                RT_DOMAIN(did)%SFCHEADAGGRT = RT_DOMAIN(did)%SFCHEADAGGRT &
                                            + RT_DOMAIN(did)%SFCHEADSUBRT(IXXRT,JYYRT)
!DJG Subgrid weighting edit...
                RT_DOMAIN(did)%LSMVOL = RT_DOMAIN(did)%LSMVOL &
                                      + RT_DOMAIN(did)%SFCHEADSUBRT(IXXRT,JYYRT) &
                                      * RT_DOMAIN(did)%dist(IXXRT,JYYRT,9)

                do KRT=1,nlst_rt(did)%NSOIL
!DJG               SMCAGGRT(KRT)=SMCAGGRT(KRT)+SMCRT(IXXRT,JYYRT,KRT)
                   RT_DOMAIN(did)%SH2OAGGRT(KRT) = RT_DOMAIN(did)%SH2OAGGRT(KRT) &
                                                 + RT_DOMAIN(did)%SMCRT(IXXRT,JYYRT,KRT)
                end do

              end do
             end do



            RT_DOMAIN(did)%SFCHEADRT(I,J) = RT_DOMAIN(did)%SFCHEADAGGRT &
                                          / (nlst_rt(did)%AGGFACTRT**2)

            do KRT=1,nlst_rt(did)%NSOIL
!DJG              SMC(I,J,KRT)=SMCAGGRT(KRT)/(AGGFACTRT**2)
               RT_DOMAIN(did)%SH2OX(I,J,KRT) = RT_DOMAIN(did)%SH2OAGGRT(KRT) &
                                             / (nlst_rt(did)%AGGFACTRT**2)
            end do



!DJG Calculate subgrid weighting array...

              do AGGFACYRT=nlst_rt(did)%AGGFACTRT-1,0,-1
                do AGGFACXRT=nlst_rt(did)%AGGFACTRT-1,0,-1
                  IXXRT=I*nlst_rt(did)%AGGFACTRT-AGGFACXRT
                  JYYRT=J*nlst_rt(did)%AGGFACTRT-AGGFACYRT
       if(left_id.ge.0) IXXRT=IXXRT+1
       if(down_id.ge.0) JYYRT=JYYRT+1
                  if (RT_DOMAIN(did)%LSMVOL.gt.0.) then
                    RT_DOMAIN(did)%INFXSWGT(IXXRT,JYYRT) &
                                          = RT_DOMAIN(did)%SFCHEADSUBRT(IXXRT,JYYRT) &
                                          * RT_DOMAIN(did)%dist(IXXRT,JYYRT,9) &
					  / RT_DOMAIN(did)%LSMVOL
                  else
                    RT_DOMAIN(did)%INFXSWGT(IXXRT,JYYRT) &
                                          = 1./FLOAT(nlst_rt(did)%AGGFACTRT**2)
                  end if

                  do KRT=1,nlst_rt(did)%NSOIL

!!!yw added for debug
                   if(RT_DOMAIN(did)%SMCRT(IXXRT,JYYRT,KRT) .lt. 0) then
                      print*, "Error negative SMCRT", RT_DOMAIN(did)%SH2OWGT(IXXRT,JYYRT,KRT), RT_DOMAIN(did)%SMCRT(IXXRT,JYYRT,KRT),RT_DOMAIN(did)%SH2OX(I,J,KRT)
                   endif
                   if(RT_DOMAIN(did)%SH2OWGT(IXXRT,JYYRT,KRT) .lt. 0) then
                      print *, "Error negative SH2OWGT", RT_DOMAIN(did)%SH2OWGT(IXXRT,JYYRT,KRT), RT_DOMAIN(did)%SMCRT(IXXRT,JYYRT,KRT),RT_DOMAIN(did)%SH2OX(I,J,KRT)
                   endif

!end 
                    IF (RT_DOMAIN(did)%SMCRT(IXXRT,JYYRT,KRT) .GT. &
                        RT_DOMAIN(did)%SMCMAXRT(IXXRT,JYYRT,KRT)) THEN

                      print *, "SMCMAX exceeded upon aggregation...", &
                           RT_DOMAIN(did)%SMCRT(IXXRT,JYYRT,KRT),  &
                           RT_DOMAIN(did)%SMCMAXRT(IXXRT,JYYRT,KRT)
                      call hydro_stop("In module_HYDRO_drv.F aggregateDomain() - "// &
                                      "SMCMAX exceeded upon aggregation.")
                    END IF
                    IF(RT_DOMAIN(did)%SH2OX(I,J,KRT).LT.0.) THEN
                      print *, "Erroneous value of SH2O...", &
                                RT_DOMAIN(did)%SH2OX(I,J,KRT),I,J,KRT
                      print *, "Error negative SH2OX", RT_DOMAIN(did)%SH2OWGT(IXXRT,JYYRT,KRT), RT_DOMAIN(did)%SMCRT(IXXRT,JYYRT,KRT),RT_DOMAIN(did)%SH2OX(I,J,KRT)
                      call hydro_stop("In module_HYDRO_drv.F aggregateDomain() "// &
                                      "- Error negative SH2OX")
                    END IF

		    IF ( RT_DOMAIN(did)%SH2OX(I,J,KRT) .gt. 0 ) THEN
                    	RT_DOMAIN(did)%SH2OWGT(IXXRT,JYYRT,KRT) &
                                 = RT_DOMAIN(did)%SMCRT(IXXRT,JYYRT,KRT) &
                                 / RT_DOMAIN(did)%SH2OX(I,J,KRT)
                    ELSE
                         print *, "Error zero SH2OX", RT_DOMAIN(did)%SH2OWGT(IXXRT,JYYRT,KRT), RT_DOMAIN(did)%SMCRT(IXXRT,JYYRT,KRT),RT_DOMAIN(did)%SH2OX(I,J,KRT)
                         RT_DOMAIN(did)%SH2OWGT(IXXRT,JYYRT,KRT) = 0.0
                    ENDIF
!?yw
                    RT_DOMAIN(did)%SH2OWGT(IXXRT,JYYRT,KRT) = max(1.0E-05, RT_DOMAIN(did)%SH2OWGT(IXXRT,JYYRT,KRT))
                  end do

                end do
              end do

         end do
        end do

        
        call MPP_LAND_COM_REAL(RT_DOMAIN(did)%INFXSWGT, &
                               RT_DOMAIN(did)%IXRT,    &
                               RT_DOMAIN(did)%JXRT, 99)
	
        do i = 1, nlst_rt(did)%NSOIL
           call MPP_LAND_COM_REAL(RT_DOMAIN(did)%SH2OWGT(:,:,i), &
                                  RT_DOMAIN(did)%IXRT, &
				  RT_DOMAIN(did)%JXRT, 99)
        end do

!DJG Update SMC with SICE (unchanged) and new value of SH2O from routing...
	RT_DOMAIN(did)%SMC = RT_DOMAIN(did)%SH2OX + RT_DOMAIN(did)%SICE

! ADCHANGE: START Final water balance variables
! ALL VARS in MM
        suminfxs2 = 0.
        smctot2 = 0.
        sicetot2 = 0.
        do i=1,RT_DOMAIN(did)%IX
         do j=1,RT_DOMAIN(did)%JX
            suminfxs2 = suminfxs2 + RT_DOMAIN(did)%SFCHEADRT(I,J) &
                               / float(RT_DOMAIN(did)%IX * RT_DOMAIN(did)%JX)
            do kk=1,nlst_rt(did)%NSOIL
                smctot2 = smctot2 + RT_DOMAIN(did)%SMC(I,J,KK)*RT_DOMAIN(did)%SLDPTH(KK)*1000. &
                               / float(RT_DOMAIN(did)%IX * RT_DOMAIN(did)%JX)
               sicetot2 = sicetot2 + RT_DOMAIN(did)%SICE(I,J,KK)*RT_DOMAIN(did)%SLDPTH(KK)*1000. &
                                / float(RT_DOMAIN(did)%IX * RT_DOMAIN(did)%JX)
            end do
         end do
        end do

! not tested
        CALL sum_real1(suminfxs2)
        CALL sum_real1(smctot2)
        CALL sum_real1(sicetot2)
        suminfxs2 = suminfxs2/float(numprocs)
        smctot2 = smctot2/float(numprocs)
        sicetot2 = sicetot2/float(numprocs)

       if (my_id .eq. IO_id) then
         print *, "Agg Mass Bal: "
         print *, "WB_AGG!InfxsDiff", suminfxs2-suminfxsrt1
         print *, "WB_AGG!Infxs1", suminfxsrt1
         print *, "WB_AGG!Infxs2", suminfxs2
         print *, "WB_AGG!SMCDiff", smctot2-smcrttot1-sicetot2
         print *, "WB_AGG!SMC1", smcrttot1
         print *, "WB_AGG!SMC2", smctot2
         print *, "WB_AGG!SICE2", sicetot2
         print *, "WB_AGG!Residual", (suminfxs2-suminfxsrt1) + &
                         (smctot2-smcrttot1-sicetot2)
	endif
! END Final water balance variables

 	print *, "Finished Aggregation..."

	
      end subroutine aggregateDomain
      
      subroutine RunOffDisag(runoff1x_in, runoff1x, area_lsm,cellArea, infxswgt, AGGFACTRT, ix,jx)
        implicit none
        real, dimension(:,:) :: runoff1x_in, runoff1x, area_lsm, cellArea, infxswgt
        integer :: i,j,ix,jx,AGGFACYRT, AGGFACXRT, AGGFACTRT, IXXRT, JYYRT

        do J=1,JX
        do I=1,IX
             do AGGFACYRT=AGGFACTRT-1,0,-1
             do AGGFACXRT=AGGFACTRT-1,0,-1
               IXXRT=I*AGGFACTRT-AGGFACXRT
               JYYRT=J*AGGFACTRT-AGGFACYRT
       if(left_id.ge.0) IXXRT=IXXRT+1
       if(down_id.ge.0) JYYRT=JYYRT+1
!DJG Implement subgrid weighting routine...
               if( (runoff1x_in(i,j) .lt. 0) .or. (runoff1x_in(i,j) .gt. 1000) ) then
                    runoff1x(IXXRT,JYYRT) = 0
               else
                    runoff1x(IXXRT,JYYRT)=runoff1x_in(i,j)*area_lsm(I,J)     &
                        *INFXSWGT(IXXRT,JYYRT)/cellArea(IXXRT,JYYRT)
               endif

             enddo
             enddo
        enddo
        enddo

      end subroutine RunOffDisag
      

subroutine HYDRO_ini(ntime, did,ix0,jx0, vegtyp,soltyp)
implicit none
integer ntime, did
integer rst_out, ix,jx
!        integer, OPTIONAL:: ix0,jx0
integer:: ix0,jx0
integer, dimension(ix0,jx0),optional :: vegtyp, soltyp
integer            :: iret, ncid, ascIndId

call  MPP_LAND_INIT()


! read the namelist
! the lsm namelist will be read by rtland sequentially again.
call read_rt_nlst(nlst_rt(did) )

if(nlst_rt(did)%rtFlag .eq. 0) return

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

! get the dimension 
call get_file_dimension(trim(nlst_rt(did)%geo_static_flnm), ix,jx)



if (nlst_rt(did)%sys_cpl .eq. 1 .or. nlst_rt(did)%sys_cpl .eq. 4) then
   !sys_cpl: 1-- coupling with HRLDAS but running offline lsm; 
   !         2-- coupling with WRF but do not run offline lsm  
   !         3-- coupling with LIS and do not run offline lsm  
   !         4:  coupling with CLM
   
   ! create 2 dimensiaon logical mapping of the CPUs for coupling with CLM or HRLDAS.
   call log_map2d()
   
   global_nx = ix  ! get from land model
   global_ny = jx  ! get from land model
   
   call mpp_land_bcast_int1(global_nx)
   call mpp_land_bcast_int1(global_ny)
   
!!! temp set global_nx to ix 
   rt_domain(did)%ix = global_nx
   rt_domain(did)%jx = global_ny
   
   ! over write the ix and jx
   call MPP_LAND_PAR_INI(1,rt_domain(did)%ix,rt_domain(did)%jx,&
        nlst_rt(did)%AGGFACTRT)
else  
   !  coupled with WRF, LIS
   numprocs = node_info(1,1)
   
   call wrf_LAND_set_INIT(node_info,numprocs,nlst_rt(did)%AGGFACTRT)
      
   rt_domain(did)%ix = local_nx
   rt_domain(did)%jx = local_ny
endif


rt_domain(did)%g_IXRT=global_rt_nx
rt_domain(did)%g_JXRT=global_rt_ny
rt_domain(did)%ixrt = local_rt_nx
rt_domain(did)%jxrt = local_rt_ny

write(6,*) "rt_domain(did)%g_IXRT, rt_domain(did)%g_JXRT, rt_domain(did)%ixrt, rt_domain(did)%jxrt"
write(6,*)  rt_domain(did)%g_IXRT, rt_domain(did)%g_JXRT, rt_domain(did)%ixrt, rt_domain(did)%jxrt
write(6,*) "rt_domain(did)%ix, rt_domain(did)%jx "
write(6,*) rt_domain(did)%ix, rt_domain(did)%jx 
write(6,*) "global_nx, global_ny, local_nx, local_ny"
write(6,*) global_nx, global_ny, local_nx, local_ny

      
!      allocate rt arrays


call getChanDim(did)


write(6,*) "finish getChanDim "

! ADCHANGE: get global attributes
! need to set these after getChanDim since it allocates rt_domain vals to 0
     call get_file_globalatts(trim(nlst_rt(did)%geo_static_flnm), &
           rt_domain(did)%iswater, rt_domain(did)%isurban, rt_domain(did)%isoilwater)

      write(6,*) "hydro_ini: rt_domain(did)%iswater, rt_domain(did)%isurban, rt_domain(did)%isoilwater"
      write(6,*) rt_domain(did)%iswater, rt_domain(did)%isurban, rt_domain(did)%isoilwater

if(nlst_rt(did)%GWBASESWCRT .eq. 3 ) then
   call gw2d_allocate(did,&
        rt_domain(did)%ixrt,&
        rt_domain(did)%jxrt,&
        nlst_rt(did)%nsoil)
   write(6,*) "finish gw2d_allocate"
endif

! calculate the distance between grids for routing.
! decompose the land parameter/data 


!      ix0= rt_domain(did)%ix
!      jx0= rt_domain(did)%jx
if(nlst_rt(did)%channel_only .eq. 0 .and. nlst_rt(did)%channelBucket_only .eq. 0) then
   if(present(vegtyp)) then
      call lsm_input(did,ix0=ix0,jx0=jx0,vegtyp0=vegtyp,soltyp0=soltyp)
   else
      call lsm_input(did,ix0=ix0,jx0=jx0)
   endif
endif


write(6,*) "finish decomposion"


if((nlst_rt(did)%channel_only .eq. 1 .or. nlst_rt(did)%channelBucket_only .eq. 1) .and. &
     nlst_rt(1)%io_form_outputs .ne. 0) then
   !! This is the "decoder ring" for reading channel-only forcing from io_form_outputs=1,2 CHRTOUT files.
   !! Only needed on io_id
   if(my_id .eq. io_id) then
      allocate(rt_domain(did)%ascendIndex(rt_domain(did)%gnlinksl))
      iret = nf90_open(trim(nlst_rt(1)%route_link_f),NF90_NOWRITE,ncid=ncid)
      !if(iret .ne. 0) call hdyro_stop
      if(iret .ne. 0) call hydro_stop('ERROR: Unable to open RouteLink file for index extraction')
      iret = nf90_inq_varid(ncid,'ascendingIndex',ascIndId)
      if(iret .ne. 0) call hydro_stop('ERROR: Unable to find ascendingIndex from RouteLink file.')
      iret = nf90_get_var(ncid,ascIndId,rt_domain(did)%ascendIndex)
      if(iret .ne. 0) call hydro_stop('ERROR: Unable to extract ascendingIndex from RouteLink file.')
      iret = nf90_close(ncid)
      if(iret .ne. 0) call hydro_stop('ERROR: Unable to close RouteLink file.')
   else 
      allocate(rt_domain(did)%ascendIndex(1))
      rt_domain(did)%ascendIndex(1)=-9
   endif
endif
  
   
call get_dist_lsm(did) !! always needed (channel_only and channelBucket_only)
if(nlst_rt(did)%channel_only .ne. 1)  call get_dist_lrt(did) !! needed forchannelBucket_only
  
! rt model initilization
call LandRT_ini(did)


if(nlst_rt(did)%GWBASESWCRT .eq. 3 ) then
   
   call gw2d_ini(did,&
        nlst_rt(did)%dt,&
        nlst_rt(did)%dxrt0)
   write(6,*) "finish gw2d_ini"      
endif
write(6,*) "finish LandRT_ini"

    
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
if(nlst_rt(did)%channel_only .eq. 0 .and. nlst_rt(did)%channelBucket_only .eq. 0) then

   if (nlst_rt(did)%TERADJ_SOLAR.eq.1 .and. nlst_rt(did)%CHANRTSWCRT.ne.2) then   ! Perform ter rain adjustment of incoming solar
      call MPP_seq_land_SO8(rt_domain(did)%SO8LD_D,rt_domain(did)%SO8LD_Vmax,&
           rt_domain(did)%TERRAIN, rt_domain(did)%dist_lsm,&
           rt_domain(did)%ix,rt_domain(did)%jx,global_nx,global_ny)
   endif
endif

if (nlst_rt(did)%GWBASESWCRT .gt. 0) then
   if(nlst_rt(did)%UDMP_OPT .eq. 1) then
      call get_basn_area_nhd(rt_domain(did)%basns_area)
   else
      call get_basn_area(did)
   endif
endif

if (nlst_rt(did)%CHANRTSWCRT.eq.1 .or. nlst_rt(did)%CHANRTSWCRT .eq. 2 ) then
   call get_node_area(did)
endif
     

if(nlst_rt(did)%CHANRTSWCRT .ne. 0) call init_stream_nudging


!    if (trim(nlst_rt(did)%restart_file) == "") then
! output at the initial time
!        call HYDRO_out(did)
!        return
!    endif

! restart the file

        ! jummp the initial time output
!        rt_domain(did)%out_counts = rt_domain(did)%out_counts + 1
!        rt_domain(did)%his_out_counts = rt_domain(did)%his_out_counts + 1


call HYDRO_rst_in(did)

!#ifdef HYDRO_REALTIME
if (trim(nlst_rt(did)%restart_file) == "") then
  call HYDRO_out(did, 0)
else
  call HYDRO_out(did, 1)
endif
!! JLM: This is only currently part 1/2 of a better accumulation tracking strategy. 
!! The parts:
!! 1) (this) zero accumulations on restart/init after any t=0 outputs are written.
!! 2) introduce a variable in the output files which indicates if accumulations are 
!!    reset after this output.
!! Here we move zeroing to after AFTER outputing the accumulations at the initial time.
!! This was previously done in HYDRO_rst_in and so output accumulations at time 
!! zero were getting zeroed and then writtent to file, which looses information. 
!! Note that nlst_rt(did)%rstrt_swc is not changed at any point in between here and the rst_in.
if(nlst_rt(did)%rstrt_swc.eq.1) then  !Switch for rest of restart accum vars...
   print *, "Resetting RESTART Accumulation Variables to 0...",nlst_rt(did)%rstrt_swc
   ! Under channel-only , these first three variables are not allocated.
   if(allocated(rt_domain(did)%LAKE_INFLORT))    rt_domain(did)%LAKE_INFLORT = zeroFlt
   if(allocated(rt_domain(did)%QSTRMVOLRT_ACC))  rt_domain(did)%QSTRMVOLRT_ACC   = zeroFlt
   ! These variables are optionally allocated, if their output is requested.
   if(allocated(rt_domain(did)%accSfcLatRunoff)) rt_domain(did)%accSfcLatRunoff = zeroDbl
   if(allocated(rt_domain(did)%accBucket))       rt_domain(did)%accBucket    = zeroDbl
end if

end subroutine HYDRO_ini

      subroutine lsm_input(did,ix0,jx0,vegtyp0,soltyp0)
         implicit none
!     NetCDF-3.
!
! netcdf version 3 fortran interface:
!

!
! external netcdf data types:
!
      integer nf_byte
      integer nf_int1
      integer nf_char
      integer nf_short
      integer nf_int2
      integer nf_int
      integer nf_float
      integer nf_real
      integer nf_double

      parameter (nf_byte = 1)
      parameter (nf_int1 = nf_byte)
      parameter (nf_char = 2)
      parameter (nf_short = 3)
      parameter (nf_int2 = nf_short)
      parameter (nf_int = 4)
      parameter (nf_float = 5)
      parameter (nf_real = nf_float)
      parameter (nf_double = 6)

!
! default fill values:
!
      integer           nf_fill_byte
      integer           nf_fill_int1
      integer           nf_fill_char
      integer           nf_fill_short
      integer           nf_fill_int2
      integer           nf_fill_int
      real              nf_fill_float
      real              nf_fill_real
      doubleprecision   nf_fill_double

      parameter (nf_fill_byte = -127)
      parameter (nf_fill_int1 = nf_fill_byte)
      parameter (nf_fill_char = 0)
      parameter (nf_fill_short = -32767)
      parameter (nf_fill_int2 = nf_fill_short)
      parameter (nf_fill_int = -2147483647)
      parameter (nf_fill_float = 9.9692099683868690e+36)
      parameter (nf_fill_real = nf_fill_float)
      parameter (nf_fill_double = 9.9692099683868690d+36)

!
! mode flags for opening and creating a netcdf dataset:
!
      integer nf_nowrite
      integer nf_write
      integer nf_clobber
      integer nf_noclobber
      integer nf_fill
      integer nf_nofill
      integer nf_lock
      integer nf_share
      integer nf_64bit_offset
      integer nf_sizehint_default
      integer nf_align_chunk
      integer nf_format_classic
      integer nf_format_64bit
      integer nf_diskless
      integer nf_mmap

      parameter (nf_nowrite = 0)
      parameter (nf_write = 1)
      parameter (nf_clobber = 0)
      parameter (nf_noclobber = 4)
      parameter (nf_fill = 0)
      parameter (nf_nofill = 256)
      parameter (nf_lock = 1024)
      parameter (nf_share = 2048)
      parameter (nf_64bit_offset = 512)
      parameter (nf_sizehint_default = 0)
      parameter (nf_align_chunk = -1)
      parameter (nf_format_classic = 1)
      parameter (nf_format_64bit = 2)
      parameter (nf_diskless = 8)
      parameter (nf_mmap = 16)

!
! size argument for defining an unlimited dimension:
!
      integer nf_unlimited
      parameter (nf_unlimited = 0)

!
! global attribute id:
!
      integer nf_global
      parameter (nf_global = 0)

!
! implementation limits:
!
      integer nf_max_dims
      integer nf_max_attrs
      integer nf_max_vars
      integer nf_max_name
      integer nf_max_var_dims

      parameter (nf_max_dims = 1024)
      parameter (nf_max_attrs = 8192)
      parameter (nf_max_vars = 8192)
      parameter (nf_max_name = 256)
      parameter (nf_max_var_dims = nf_max_dims)

!
! error codes:
!
      integer nf_noerr
      integer nf_ebadid
      integer nf_eexist
      integer nf_einval
      integer nf_eperm
      integer nf_enotindefine
      integer nf_eindefine
      integer nf_einvalcoords
      integer nf_emaxdims
      integer nf_enameinuse
      integer nf_enotatt
      integer nf_emaxatts
      integer nf_ebadtype
      integer nf_ebaddim
      integer nf_eunlimpos
      integer nf_emaxvars
      integer nf_enotvar
      integer nf_eglobal
      integer nf_enotnc
      integer nf_ests
      integer nf_emaxname
      integer nf_eunlimit
      integer nf_enorecvars
      integer nf_echar
      integer nf_eedge
      integer nf_estride
      integer nf_ebadname
      integer nf_erange
      integer nf_enomem
      integer nf_evarsize
      integer nf_edimsize
      integer nf_etrunc

      parameter (nf_noerr = 0)
      parameter (nf_ebadid = -33)
      parameter (nf_eexist = -35)
      parameter (nf_einval = -36)
      parameter (nf_eperm = -37)
      parameter (nf_enotindefine = -38)
      parameter (nf_eindefine = -39)
      parameter (nf_einvalcoords = -40)
      parameter (nf_emaxdims = -41)
      parameter (nf_enameinuse = -42)
      parameter (nf_enotatt = -43)
      parameter (nf_emaxatts = -44)
      parameter (nf_ebadtype = -45)
      parameter (nf_ebaddim = -46)
      parameter (nf_eunlimpos = -47)
      parameter (nf_emaxvars = -48)
      parameter (nf_enotvar = -49)
      parameter (nf_eglobal = -50)
      parameter (nf_enotnc = -51)
      parameter (nf_ests = -52)
      parameter (nf_emaxname = -53)
      parameter (nf_eunlimit = -54)
      parameter (nf_enorecvars = -55)
      parameter (nf_echar = -56)
      parameter (nf_eedge = -57)
      parameter (nf_estride = -58)
      parameter (nf_ebadname = -59)
      parameter (nf_erange = -60)
      parameter (nf_enomem = -61)
      parameter (nf_evarsize = -62)
      parameter (nf_edimsize = -63)
      parameter (nf_etrunc = -64)
!
! error handling modes:
!
      integer  nf_fatal
      integer nf_verbose

      parameter (nf_fatal = 1)
      parameter (nf_verbose = 2)

!
! miscellaneous routines:
!
      character*80   nf_inq_libvers
      external       nf_inq_libvers

      character*80   nf_strerror
!                         (integer             ncerr)
      external       nf_strerror

      logical        nf_issyserr
!                         (integer             ncerr)
      external       nf_issyserr

!
! control routines:
!
      integer         nf_inq_base_pe
!                         (integer             ncid,
!                          integer             pe)
      external        nf_inq_base_pe

      integer         nf_set_base_pe
!                         (integer             ncid,
!                          integer             pe)
      external        nf_set_base_pe

      integer         nf_create
!                         (character*(*)       path,
!                          integer             cmode,
!                          integer             ncid)
      external        nf_create

      integer         nf__create
!                         (character*(*)       path,
!                          integer             cmode,
!                          integer             initialsz,
!                          integer             chunksizehint,
!                          integer             ncid)
      external        nf__create

      integer         nf__create_mp
!                         (character*(*)       path,
!                          integer             cmode,
!                          integer             initialsz,
!                          integer             basepe,
!                          integer             chunksizehint,
!                          integer             ncid)
      external        nf__create_mp

      integer         nf_open
!                         (character*(*)       path,
!                          integer             mode,
!                          integer             ncid)
      external        nf_open

      integer         nf__open
!                         (character*(*)       path,
!                          integer             mode,
!                          integer             chunksizehint,
!                          integer             ncid)
      external        nf__open

      integer         nf__open_mp
!                         (character*(*)       path,
!                          integer             mode,
!                          integer             basepe,
!                          integer             chunksizehint,
!                          integer             ncid)
      external        nf__open_mp

      integer         nf_set_fill
!                         (integer             ncid,
!                          integer             fillmode,
!                          integer             old_mode)
      external        nf_set_fill

      integer         nf_set_default_format
!                          (integer             format,
!                          integer             old_format)
      external        nf_set_default_format

      integer         nf_redef
!                         (integer             ncid)
      external        nf_redef

      integer         nf_enddef
!                         (integer             ncid)
      external        nf_enddef

      integer         nf__enddef
!                         (integer             ncid,
!                          integer             h_minfree,
!                          integer             v_align,
!                          integer             v_minfree,
!                          integer             r_align)
      external        nf__enddef

      integer         nf_sync
!                         (integer             ncid)
      external        nf_sync

      integer         nf_abort
!                         (integer             ncid)
      external        nf_abort

      integer         nf_close
!                         (integer             ncid)
      external        nf_close

      integer         nf_delete
!                         (character*(*)       ncid)
      external        nf_delete

!
! general inquiry routines:
!

      integer         nf_inq
!                         (integer             ncid,
!                          integer             ndims,
!                          integer             nvars,
!                          integer             ngatts,
!                          integer             unlimdimid)
      external        nf_inq

! new inquire path

      integer nf_inq_path
      external nf_inq_path

      integer         nf_inq_ndims
!                         (integer             ncid,
!                          integer             ndims)
      external        nf_inq_ndims

      integer         nf_inq_nvars
!                         (integer             ncid,
!                          integer             nvars)
      external        nf_inq_nvars

      integer         nf_inq_natts
!                         (integer             ncid,
!                          integer             ngatts)
      external        nf_inq_natts

      integer         nf_inq_unlimdim
!                         (integer             ncid,
!                          integer             unlimdimid)
      external        nf_inq_unlimdim

      integer         nf_inq_format
!                         (integer             ncid,
!                          integer             format)
      external        nf_inq_format

!
! dimension routines:
!

      integer         nf_def_dim
!                         (integer             ncid,
!                          character(*)        name,
!                          integer             len,
!                          integer             dimid)
      external        nf_def_dim

      integer         nf_inq_dimid
!                         (integer             ncid,
!                          character(*)        name,
!                          integer             dimid)
      external        nf_inq_dimid

      integer         nf_inq_dim
!                         (integer             ncid,
!                          integer             dimid,
!                          character(*)        name,
!                          integer             len)
      external        nf_inq_dim

      integer         nf_inq_dimname
!                         (integer             ncid,
!                          integer             dimid,
!                          character(*)        name)
      external        nf_inq_dimname

      integer         nf_inq_dimlen
!                         (integer             ncid,
!                          integer             dimid,
!                          integer             len)
      external        nf_inq_dimlen

      integer         nf_rename_dim
!                         (integer             ncid,
!                          integer             dimid,
!                          character(*)        name)
      external        nf_rename_dim

!
! general attribute routines:
!

      integer         nf_inq_att
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             xtype,
!                          integer             len)
      external        nf_inq_att

      integer         nf_inq_attid
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             attnum)
      external        nf_inq_attid

      integer         nf_inq_atttype
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             xtype)
      external        nf_inq_atttype

      integer         nf_inq_attlen
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             len)
      external        nf_inq_attlen

      integer         nf_inq_attname
!                         (integer             ncid,
!                          integer             varid,
!                          integer             attnum,
!                          character(*)        name)
      external        nf_inq_attname

      integer         nf_copy_att
!                         (integer             ncid_in,
!                          integer             varid_in,
!                          character(*)        name,
!                          integer             ncid_out,
!                          integer             varid_out)
      external        nf_copy_att

      integer         nf_rename_att
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        curname,
!                          character(*)        newname)
      external        nf_rename_att

      integer         nf_del_att
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name)
      external        nf_del_att

!
! attribute put/get routines:
!

      integer         nf_put_att_text
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             len,
!                          character(*)        text)
      external        nf_put_att_text

      integer         nf_get_att_text
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          character(*)        text)
      external        nf_get_att_text

      integer         nf_put_att_int1
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             xtype,
!                          integer             len,
!                          nf_int1_t           i1vals(1))
      external        nf_put_att_int1

      integer         nf_get_att_int1
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          nf_int1_t           i1vals(1))
      external        nf_get_att_int1

      integer         nf_put_att_int2
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             xtype,
!                          integer             len,
!                          nf_int2_t           i2vals(1))
      external        nf_put_att_int2

      integer         nf_get_att_int2
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          nf_int2_t           i2vals(1))
      external        nf_get_att_int2

      integer         nf_put_att_int
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             xtype,
!                          integer             len,
!                          integer             ivals(1))
      external        nf_put_att_int

      integer         nf_get_att_int
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             ivals(1))
      external        nf_get_att_int

      integer         nf_put_att_real
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             xtype,
!                          integer             len,
!                          real                rvals(1))
      external        nf_put_att_real

      integer         nf_get_att_real
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          real                rvals(1))
      external        nf_get_att_real

      integer         nf_put_att_double
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             xtype,
!                          integer             len,
!                          double              dvals(1))
      external        nf_put_att_double

      integer         nf_get_att_double
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          double              dvals(1))
      external        nf_get_att_double

!
! general variable routines:
!

      integer         nf_def_var
!                         (integer             ncid,
!                          character(*)        name,
!                          integer             datatype,
!                          integer             ndims,
!                          integer             dimids(1),
!                          integer             varid)
      external        nf_def_var

      integer         nf_inq_var
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name,
!                          integer             datatype,
!                          integer             ndims,
!                          integer             dimids(1),
!                          integer             natts)
      external        nf_inq_var

      integer         nf_inq_varid
!                         (integer             ncid,
!                          character(*)        name,
!                          integer             varid)
      external        nf_inq_varid

      integer         nf_inq_varname
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name)
      external        nf_inq_varname

      integer         nf_inq_vartype
!                         (integer             ncid,
!                          integer             varid,
!                          integer             xtype)
      external        nf_inq_vartype

      integer         nf_inq_varndims
!                         (integer             ncid,
!                          integer             varid,
!                          integer             ndims)
      external        nf_inq_varndims

      integer         nf_inq_vardimid
!                         (integer             ncid,
!                          integer             varid,
!                          integer             dimids(1))
      external        nf_inq_vardimid

      integer         nf_inq_varnatts
!                         (integer             ncid,
!                          integer             varid,
!                          integer             natts)
      external        nf_inq_varnatts

      integer         nf_rename_var
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        name)
      external        nf_rename_var

      integer         nf_copy_var
!                         (integer             ncid_in,
!                          integer             varid,
!                          integer             ncid_out)
      external        nf_copy_var

!
! entire variable put/get routines:
!

      integer         nf_put_var_text
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        text)
      external        nf_put_var_text

      integer         nf_get_var_text
!                         (integer             ncid,
!                          integer             varid,
!                          character(*)        text)
      external        nf_get_var_text

      integer         nf_put_var_int1
!                         (integer             ncid,
!                          integer             varid,
!                          nf_int1_t           i1vals(1))
      external        nf_put_var_int1

      integer         nf_get_var_int1
!                         (integer             ncid,
!                          integer             varid,
!                          nf_int1_t           i1vals(1))
      external        nf_get_var_int1

      integer         nf_put_var_int2
!                         (integer             ncid,
!                          integer             varid,
!                          nf_int2_t           i2vals(1))
      external        nf_put_var_int2

      integer         nf_get_var_int2
!                         (integer             ncid,
!                          integer             varid,
!                          nf_int2_t           i2vals(1))
      external        nf_get_var_int2

      integer         nf_put_var_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             ivals(1))
      external        nf_put_var_int

      integer         nf_get_var_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             ivals(1))
      external        nf_get_var_int

      integer         nf_put_var_real
!                         (integer             ncid,
!                          integer             varid,
!                          real                rvals(1))
      external        nf_put_var_real

      integer         nf_get_var_real
!                         (integer             ncid,
!                          integer             varid,
!                          real                rvals(1))
      external        nf_get_var_real

      integer         nf_put_var_double
!                         (integer             ncid,
!                          integer             varid,
!                          doubleprecision     dvals(1))
      external        nf_put_var_double

      integer         nf_get_var_double
!                         (integer             ncid,
!                          integer             varid,
!                          doubleprecision     dvals(1))
      external        nf_get_var_double

!
! single variable put/get routines:
!

      integer         nf_put_var1_text
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          character*1         text)
      external        nf_put_var1_text

      integer         nf_get_var1_text
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          character*1         text)
      external        nf_get_var1_text

      integer         nf_put_var1_int1
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          nf_int1_t           i1val)
      external        nf_put_var1_int1

      integer         nf_get_var1_int1
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          nf_int1_t           i1val)
      external        nf_get_var1_int1

      integer         nf_put_var1_int2
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          nf_int2_t           i2val)
      external        nf_put_var1_int2

      integer         nf_get_var1_int2
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          nf_int2_t           i2val)
      external        nf_get_var1_int2

      integer         nf_put_var1_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          integer             ival)
      external        nf_put_var1_int

      integer         nf_get_var1_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          integer             ival)
      external        nf_get_var1_int

      integer         nf_put_var1_real
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          real                rval)
      external        nf_put_var1_real

      integer         nf_get_var1_real
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          real                rval)
      external        nf_get_var1_real

      integer         nf_put_var1_double
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          doubleprecision     dval)
      external        nf_put_var1_double

      integer         nf_get_var1_double
!                         (integer             ncid,
!                          integer             varid,
!                          integer             index(1),
!                          doubleprecision     dval)
      external        nf_get_var1_double

!
! variable array put/get routines:
!

      integer         nf_put_vara_text
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          character(*)        text)
      external        nf_put_vara_text

      integer         nf_get_vara_text
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          character(*)        text)
      external        nf_get_vara_text

      integer         nf_put_vara_int1
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          nf_int1_t           i1vals(1))
      external        nf_put_vara_int1

      integer         nf_get_vara_int1
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          nf_int1_t           i1vals(1))
      external        nf_get_vara_int1

      integer         nf_put_vara_int2
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          nf_int2_t           i2vals(1))
      external        nf_put_vara_int2

      integer         nf_get_vara_int2
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          nf_int2_t           i2vals(1))
      external        nf_get_vara_int2

      integer         nf_put_vara_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             ivals(1))
      external        nf_put_vara_int

      integer         nf_get_vara_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             ivals(1))
      external        nf_get_vara_int

      integer         nf_put_vara_real
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          real                rvals(1))
      external        nf_put_vara_real

      integer         nf_get_vara_real
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          real                rvals(1))
      external        nf_get_vara_real

      integer         nf_put_vara_double
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          doubleprecision     dvals(1))
      external        nf_put_vara_double

      integer         nf_get_vara_double
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          doubleprecision     dvals(1))
      external        nf_get_vara_double

!
! strided variable put/get routines:
!

      integer         nf_put_vars_text
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          character(*)        text)
      external        nf_put_vars_text

      integer         nf_get_vars_text
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          character(*)        text)
      external        nf_get_vars_text

      integer         nf_put_vars_int1
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          nf_int1_t           i1vals(1))
      external        nf_put_vars_int1

      integer         nf_get_vars_int1
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          nf_int1_t           i1vals(1))
      external        nf_get_vars_int1

      integer         nf_put_vars_int2
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          nf_int2_t           i2vals(1))
      external        nf_put_vars_int2

      integer         nf_get_vars_int2
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          nf_int2_t           i2vals(1))
      external        nf_get_vars_int2

      integer         nf_put_vars_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             ivals(1))
      external        nf_put_vars_int

      integer         nf_get_vars_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             ivals(1))
      external        nf_get_vars_int

      integer         nf_put_vars_real
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          real                rvals(1))
      external        nf_put_vars_real

      integer         nf_get_vars_real
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          real                rvals(1))
      external        nf_get_vars_real

      integer         nf_put_vars_double
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          doubleprecision     dvals(1))
      external        nf_put_vars_double

      integer         nf_get_vars_double
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          doubleprecision     dvals(1))
      external        nf_get_vars_double

!
! mapped variable put/get routines:
!

      integer         nf_put_varm_text
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          character(*)        text)
      external        nf_put_varm_text

      integer         nf_get_varm_text
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          character(*)        text)
      external        nf_get_varm_text

      integer         nf_put_varm_int1
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          nf_int1_t           i1vals(1))
      external        nf_put_varm_int1

      integer         nf_get_varm_int1
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          nf_int1_t           i1vals(1))
      external        nf_get_varm_int1

      integer         nf_put_varm_int2
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          nf_int2_t           i2vals(1))
      external        nf_put_varm_int2

      integer         nf_get_varm_int2
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          nf_int2_t           i2vals(1))
      external        nf_get_varm_int2

      integer         nf_put_varm_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          integer             ivals(1))
      external        nf_put_varm_int

      integer         nf_get_varm_int
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          integer             ivals(1))
      external        nf_get_varm_int

      integer         nf_put_varm_real
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          real                rvals(1))
      external        nf_put_varm_real

      integer         nf_get_varm_real
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          real                rvals(1))
      external        nf_get_varm_real

      integer         nf_put_varm_double
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          doubleprecision     dvals(1))
      external        nf_put_varm_double

      integer         nf_get_varm_double
!                         (integer             ncid,
!                          integer             varid,
!                          integer             start(1),
!                          integer             count(1),
!                          integer             stride(1),
!                          integer             imap(1),
!                          doubleprecision     dvals(1))
      external        nf_get_varm_double


!     NetCDF-4.
!     This is part of netCDF-4. Copyright 2006, UCAR, See COPYRIGHT
!     file for distribution information.

!     Netcdf version 4 fortran interface.

!     $Id: netcdf4.inc,v 1.28 2010/05/25 13:53:02 ed Exp $

!     New netCDF-4 types.
      integer nf_ubyte
      integer nf_ushort
      integer nf_uint
      integer nf_int64
      integer nf_uint64
      integer nf_string
      integer nf_vlen
      integer nf_opaque
      integer nf_enum
      integer nf_compound

      parameter (nf_ubyte = 7)
      parameter (nf_ushort = 8)
      parameter (nf_uint = 9)
      parameter (nf_int64 = 10)
      parameter (nf_uint64 = 11)
      parameter (nf_string = 12)
      parameter (nf_vlen = 13)
      parameter (nf_opaque = 14)
      parameter (nf_enum = 15)
      parameter (nf_compound = 16)

!     New netCDF-4 fill values.
      integer           nf_fill_ubyte
      integer           nf_fill_ushort
!      real              nf_fill_uint
!      real              nf_fill_int64
!      real              nf_fill_uint64
      parameter (nf_fill_ubyte = 255)
      parameter (nf_fill_ushort = 65535)

!     New constants.
      integer nf_format_netcdf4
      parameter (nf_format_netcdf4 = 3)

      integer nf_format_netcdf4_classic
      parameter (nf_format_netcdf4_classic = 4)

      integer nf_netcdf4
      parameter (nf_netcdf4 = 4096)

      integer nf_classic_model
      parameter (nf_classic_model = 256)

      integer nf_chunk_seq
      parameter (nf_chunk_seq = 0)
      integer nf_chunk_sub
      parameter (nf_chunk_sub = 1)
      integer nf_chunk_sizes
      parameter (nf_chunk_sizes = 2)

      integer nf_endian_native
      parameter (nf_endian_native = 0)
      integer nf_endian_little
      parameter (nf_endian_little = 1)
      integer nf_endian_big
      parameter (nf_endian_big = 2)

!     For NF_DEF_VAR_CHUNKING
      integer nf_chunked
      parameter (nf_chunked = 0)
      integer nf_contiguous
      parameter (nf_contiguous = 1)

!     For NF_DEF_VAR_FLETCHER32
      integer nf_nochecksum
      parameter (nf_nochecksum = 0)
      integer nf_fletcher32
      parameter (nf_fletcher32 = 1)

!     For NF_DEF_VAR_DEFLATE
      integer nf_noshuffle
      parameter (nf_noshuffle = 0)
      integer nf_shuffle
      parameter (nf_shuffle = 1)

!     For NF_DEF_VAR_SZIP
      integer nf_szip_ec_option_mask
      parameter (nf_szip_ec_option_mask = 4)
      integer nf_szip_nn_option_mask
      parameter (nf_szip_nn_option_mask = 32)

!     For parallel I/O.
      integer nf_mpiio      
      parameter (nf_mpiio = 8192)
      integer nf_mpiposix
      parameter (nf_mpiposix = 16384)
      integer nf_pnetcdf
      parameter (nf_pnetcdf = 32768)

!     For NF_VAR_PAR_ACCESS.
      integer nf_independent
      parameter (nf_independent = 0)
      integer nf_collective
      parameter (nf_collective = 1)

!     New error codes.
      integer nf_ehdferr        ! Error at HDF5 layer. 
      parameter (nf_ehdferr = -101)
      integer nf_ecantread      ! Can't read. 
      parameter (nf_ecantread = -102)
      integer nf_ecantwrite     ! Can't write. 
      parameter (nf_ecantwrite = -103)
      integer nf_ecantcreate    ! Can't create. 
      parameter (nf_ecantcreate = -104)
      integer nf_efilemeta      ! Problem with file metadata. 
      parameter (nf_efilemeta = -105)
      integer nf_edimmeta       ! Problem with dimension metadata. 
      parameter (nf_edimmeta = -106)
      integer nf_eattmeta       ! Problem with attribute metadata. 
      parameter (nf_eattmeta = -107)
      integer nf_evarmeta       ! Problem with variable metadata. 
      parameter (nf_evarmeta = -108)
      integer nf_enocompound    ! Not a compound type. 
      parameter (nf_enocompound = -109)
      integer nf_eattexists     ! Attribute already exists. 
      parameter (nf_eattexists = -110)
      integer nf_enotnc4        ! Attempting netcdf-4 operation on netcdf-3 file.   
      parameter (nf_enotnc4 = -111)
      integer nf_estrictnc3     ! Attempting netcdf-4 operation on strict nc3 netcdf-4 file.   
      parameter (nf_estrictnc3 = -112)
      integer nf_enotnc3        ! Attempting netcdf-3 operation on netcdf-4 file.   
      parameter (nf_enotnc3 = -113)
      integer nf_enopar         ! Parallel operation on file opened for non-parallel access.   
      parameter (nf_enopar = -114)
      integer nf_eparinit       ! Error initializing for parallel access.   
      parameter (nf_eparinit = -115)
      integer nf_ebadgrpid      ! Bad group ID.   
      parameter (nf_ebadgrpid = -116)
      integer nf_ebadtypid      ! Bad type ID.   
      parameter (nf_ebadtypid = -117)
      integer nf_etypdefined    ! Type has already been defined and may not be edited. 
      parameter (nf_etypdefined = -118)
      integer nf_ebadfield      ! Bad field ID.   
      parameter (nf_ebadfield = -119)
      integer nf_ebadclass      ! Bad class.   
      parameter (nf_ebadclass = -120)
      integer nf_emaptype       ! Mapped access for atomic types only.   
      parameter (nf_emaptype = -121)
      integer nf_elatefill      ! Attempt to define fill value when data already exists. 
      parameter (nf_elatefill = -122)
      integer nf_elatedef       ! Attempt to define var properties, like deflate, after enddef. 
      parameter (nf_elatedef = -123)
      integer nf_edimscale      ! Probem with HDF5 dimscales. 
      parameter (nf_edimscale = -124)
      integer nf_enogrp       ! No group found.
      parameter (nf_enogrp = -125)


!     New functions.

!     Parallel I/O.
      integer nf_create_par
      external nf_create_par

      integer nf_open_par
      external nf_open_par

      integer nf_var_par_access
      external nf_var_par_access

!     Functions to handle groups.
      integer nf_inq_ncid
      external nf_inq_ncid

      integer nf_inq_grps
      external nf_inq_grps

      integer nf_inq_grpname
      external nf_inq_grpname

      integer nf_inq_grpname_full
      external nf_inq_grpname_full

      integer nf_inq_grpname_len
      external nf_inq_grpname_len

      integer nf_inq_grp_parent
      external nf_inq_grp_parent

      integer nf_inq_grp_ncid
      external nf_inq_grp_ncid

      integer nf_inq_grp_full_ncid
      external nf_inq_grp_full_ncid

      integer nf_inq_varids
      external nf_inq_varids

      integer nf_inq_dimids
      external nf_inq_dimids

      integer nf_def_grp
      external nf_def_grp

!     New rename grp function

      integer nf_rename_grp
      external nf_rename_grp

!     New options for netCDF variables.
      integer nf_def_var_deflate
      external nf_def_var_deflate

      integer nf_inq_var_deflate
      external nf_inq_var_deflate

      integer nf_def_var_fletcher32
      external nf_def_var_fletcher32

      integer nf_inq_var_fletcher32
      external nf_inq_var_fletcher32

      integer nf_def_var_chunking
      external nf_def_var_chunking

      integer nf_inq_var_chunking
      external nf_inq_var_chunking

      integer nf_def_var_fill
      external nf_def_var_fill

      integer nf_inq_var_fill
      external nf_inq_var_fill

      integer nf_def_var_endian
      external nf_def_var_endian

      integer nf_inq_var_endian
      external nf_inq_var_endian

!     User defined types.
      integer nf_inq_typeids
      external nf_inq_typeids

      integer nf_inq_typeid
      external nf_inq_typeid

      integer nf_inq_type
      external nf_inq_type

      integer nf_inq_user_type
      external nf_inq_user_type

!     User defined types - compound types.
      integer nf_def_compound
      external nf_def_compound

      integer nf_insert_compound
      external nf_insert_compound

      integer nf_insert_array_compound
      external nf_insert_array_compound

      integer nf_inq_compound
      external nf_inq_compound

      integer nf_inq_compound_name
      external nf_inq_compound_name

      integer nf_inq_compound_size
      external nf_inq_compound_size

      integer nf_inq_compound_nfields
      external nf_inq_compound_nfields

      integer nf_inq_compound_field
      external nf_inq_compound_field

      integer nf_inq_compound_fieldname
      external nf_inq_compound_fieldname

      integer nf_inq_compound_fieldindex
      external nf_inq_compound_fieldindex

      integer nf_inq_compound_fieldoffset
      external nf_inq_compound_fieldoffset

      integer nf_inq_compound_fieldtype
      external nf_inq_compound_fieldtype

      integer nf_inq_compound_fieldndims
      external nf_inq_compound_fieldndims

      integer nf_inq_compound_fielddim_sizes
      external nf_inq_compound_fielddim_sizes

!     User defined types - variable length arrays.
      integer nf_def_vlen
      external nf_def_vlen

      integer nf_inq_vlen
      external nf_inq_vlen

      integer nf_free_vlen
      external nf_free_vlen

!     User defined types - enums.
      integer nf_def_enum
      external nf_def_enum

      integer nf_insert_enum
      external nf_insert_enum

      integer nf_inq_enum
      external nf_inq_enum

      integer nf_inq_enum_member
      external nf_inq_enum_member

      integer nf_inq_enum_ident
      external nf_inq_enum_ident

!     User defined types - opaque.
      integer nf_def_opaque
      external nf_def_opaque

      integer nf_inq_opaque
      external nf_inq_opaque

!     Write and read attributes of any type, including user defined
!     types.
      integer nf_put_att
      external nf_put_att
      integer nf_get_att
      external nf_get_att

!     Write and read variables of any type, including user defined
!     types.
      integer nf_put_var
      external nf_put_var
      integer nf_put_var1
      external nf_put_var1
      integer nf_put_vara
      external nf_put_vara
      integer nf_put_vars
      external nf_put_vars
      integer nf_get_var
      external nf_get_var
      integer nf_get_var1
      external nf_get_var1
      integer nf_get_vara
      external nf_get_vara
      integer nf_get_vars
      external nf_get_vars

!     64-bit int functions.
      integer nf_put_var1_int64
      external nf_put_var1_int64
      integer nf_put_vara_int64
      external nf_put_vara_int64
      integer nf_put_vars_int64
      external nf_put_vars_int64
      integer nf_put_varm_int64
      external nf_put_varm_int64
      integer nf_put_var_int64
      external nf_put_var_int64
      integer nf_get_var1_int64
      external nf_get_var1_int64
      integer nf_get_vara_int64
      external nf_get_vara_int64
      integer nf_get_vars_int64
      external nf_get_vars_int64
      integer nf_get_varm_int64
      external nf_get_varm_int64
      integer nf_get_var_int64
      external nf_get_var_int64

!     For helping F77 users with VLENs.
      integer nf_get_vlen_element
      external nf_get_vlen_element
      integer nf_put_vlen_element
      external nf_put_vlen_element

!     For dealing with file level chunk cache.
      integer nf_set_chunk_cache
      external nf_set_chunk_cache
      integer nf_get_chunk_cache
      external nf_get_chunk_cache

!     For dealing with per variable chunk cache.
      integer nf_set_var_chunk_cache
      external nf_set_var_chunk_cache
      integer nf_get_var_chunk_cache
      external nf_get_var_chunk_cache

!     NetCDF-2.
!ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
! begin netcdf 2.4 backward compatibility:
!

!      
! functions in the fortran interface
!
      integer nccre
      integer ncopn
      integer ncddef
      integer ncdid
      integer ncvdef
      integer ncvid
      integer nctlen
      integer ncsfil

      external nccre
      external ncopn
      external ncddef
      external ncdid
      external ncvdef
      external ncvid
      external nctlen
      external ncsfil


      integer ncrdwr
      integer nccreat
      integer ncexcl
      integer ncindef
      integer ncnsync
      integer nchsync
      integer ncndirty
      integer nchdirty
      integer nclink
      integer ncnowrit
      integer ncwrite
      integer ncclob
      integer ncnoclob
      integer ncglobal
      integer ncfill
      integer ncnofill
      integer maxncop
      integer maxncdim
      integer maxncatt
      integer maxncvar
      integer maxncnam
      integer maxvdims
      integer ncnoerr
      integer ncebadid
      integer ncenfile
      integer nceexist
      integer nceinval
      integer nceperm
      integer ncenotin
      integer nceindef
      integer ncecoord
      integer ncemaxds
      integer ncename
      integer ncenoatt
      integer ncemaxat
      integer ncebadty
      integer ncebadd
      integer ncests
      integer nceunlim
      integer ncemaxvs
      integer ncenotvr
      integer nceglob
      integer ncenotnc
      integer ncfoobar
      integer ncsyserr
      integer ncfatal
      integer ncverbos
      integer ncentool


!
! netcdf data types:
!
      integer ncbyte
      integer ncchar
      integer ncshort
      integer nclong
      integer ncfloat
      integer ncdouble

      parameter(ncbyte = 1)
      parameter(ncchar = 2)
      parameter(ncshort = 3)
      parameter(nclong = 4)
      parameter(ncfloat = 5)
      parameter(ncdouble = 6)

!     
!     masks for the struct nc flag field; passed in as 'mode' arg to
!     nccreate and ncopen.
!     

!     read/write, 0 => readonly 
      parameter(ncrdwr = 1)
!     in create phase, cleared by ncendef 
      parameter(nccreat = 2)
!     on create destroy existing file 
      parameter(ncexcl = 4)
!     in define mode, cleared by ncendef 
      parameter(ncindef = 8)
!     synchronise numrecs on change (x'10')
      parameter(ncnsync = 16)
!     synchronise whole header on change (x'20')
      parameter(nchsync = 32)
!     numrecs has changed (x'40')
      parameter(ncndirty = 64)  
!     header info has changed (x'80')
      parameter(nchdirty = 128)
!     prefill vars on endef and increase of record, the default behavior
      parameter(ncfill = 0)
!     do not fill vars on endef and increase of record (x'100')
      parameter(ncnofill = 256)
!     isa link (x'8000')
      parameter(nclink = 32768)

!     
!     'mode' arguments for nccreate and ncopen
!     
      parameter(ncnowrit = 0)
      parameter(ncwrite = ncrdwr)
      parameter(ncclob = nf_clobber)
      parameter(ncnoclob = nf_noclobber)

!     
!     'size' argument to ncdimdef for an unlimited dimension
!     
      integer ncunlim
      parameter(ncunlim = 0)

!     
!     attribute id to put/get a global attribute
!     
      parameter(ncglobal  = 0)

!     
!     advisory maximums:
!     
      parameter(maxncop = 64)
      parameter(maxncdim = 1024)
      parameter(maxncatt = 8192)
      parameter(maxncvar = 8192)
!     not enforced 
      parameter(maxncnam = 256)
      parameter(maxvdims = maxncdim)

!     
!     global netcdf error status variable
!     initialized in error.c
!     

!     no error 
      parameter(ncnoerr = nf_noerr)
!     not a netcdf id 
      parameter(ncebadid = nf_ebadid)
!     too many netcdfs open 
      parameter(ncenfile = -31)   ! nc_syserr
!     netcdf file exists && ncnoclob
      parameter(nceexist = nf_eexist)
!     invalid argument 
      parameter(nceinval = nf_einval)
!     write to read only 
      parameter(nceperm = nf_eperm)
!     operation not allowed in data mode 
      parameter(ncenotin = nf_enotindefine )   
!     operation not allowed in define mode 
      parameter(nceindef = nf_eindefine)   
!     coordinates out of domain 
      parameter(ncecoord = nf_einvalcoords)
!     maxncdims exceeded 
      parameter(ncemaxds = nf_emaxdims)
!     string match to name in use 
      parameter(ncename = nf_enameinuse)   
!     attribute not found 
      parameter(ncenoatt = nf_enotatt)
!     maxncattrs exceeded 
      parameter(ncemaxat = nf_emaxatts)
!     not a netcdf data type 
      parameter(ncebadty = nf_ebadtype)
!     invalid dimension id 
      parameter(ncebadd = nf_ebaddim)
!     ncunlimited in the wrong index 
      parameter(nceunlim = nf_eunlimpos)
!     maxncvars exceeded 
      parameter(ncemaxvs = nf_emaxvars)
!     variable not found 
      parameter(ncenotvr = nf_enotvar)
!     action prohibited on ncglobal varid 
      parameter(nceglob = nf_eglobal)
!     not a netcdf file 
      parameter(ncenotnc = nf_enotnc)
      parameter(ncests = nf_ests)
      parameter (ncentool = nf_emaxname) 
      parameter(ncfoobar = 32)
      parameter(ncsyserr = -31)

!     
!     global options variable. used to determine behavior of error handler.
!     initialized in lerror.c
!     
      parameter(ncfatal = 1)
      parameter(ncverbos = 2)

!
!     default fill values.  these must be the same as in the c interface.
!
      integer filbyte
      integer filchar
      integer filshort
      integer fillong
      real filfloat
      doubleprecision fildoub

      parameter (filbyte = -127)
      parameter (filchar = 0)
      parameter (filshort = -32767)
      parameter (fillong = -2147483647)
      parameter (filfloat = 9.9692099683868690e+36)
      parameter (fildoub = 9.9692099683868690e+36)
         integer did, leng, ncid, ierr_flg
         parameter(leng=100)
         integer :: i,j, nn
         integer, allocatable, dimension(:,:) :: soltyp
         real, dimension(leng) :: xdum1, MAXSMC,refsmc,wltsmc

        integer :: ix0,jx0
        integer, dimension(ix0,jx0),OPTIONAL :: vegtyp0, soltyp0
        integer :: iret, istmp

         write(6,*) RT_DOMAIN(did)%ix,RT_DOMAIN(did)%jx

         allocate(soltyp(RT_DOMAIN(did)%ix,RT_DOMAIN(did)%jx) )

         soltyp = 0
         call get2d_lsm_soltyp(soltyp,RT_DOMAIN(did)%ix,RT_DOMAIN(did)%jx,trim(nlst_rt(did)%geo_static_flnm))


         call get2d_lsm_real("HGT",RT_DOMAIN(did)%TERRAIN,RT_DOMAIN(did)%ix,RT_DOMAIN(did)%jx,trim(nlst_rt(did)%geo_static_flnm))

         call get2d_lsm_real("XLAT",RT_DOMAIN(did)%lat_lsm,RT_DOMAIN(did)%ix,RT_DOMAIN(did)%jx,trim(nlst_rt(did)%geo_static_flnm))
         call get2d_lsm_real("XLONG",RT_DOMAIN(did)%lon_lsm,RT_DOMAIN(did)%ix,RT_DOMAIN(did)%jx,trim(nlst_rt(did)%geo_static_flnm))
         call get2d_lsm_vegtyp(RT_DOMAIN(did)%VEGTYP,RT_DOMAIN(did)%ix,RT_DOMAIN(did)%jx,trim(nlst_rt(did)%geo_static_flnm))


            if(nlst_rt(did)%sys_cpl .eq. 2 ) then
              ! coupling with WRF
                if(present(soltyp0) ) then
                  where(soltyp0 == rt_domain(did)%isoilwater) VEGTYP0 = rt_domain(did)%iswater
                  where(VEGTYP0 == rt_domain(did)%iswater ) soltyp0 = rt_domain(did)%isoilwater 
                   soltyp = soltyp0
                   RT_DOMAIN(did)%VEGTYP = VEGTYP0
                endif
            endif

         where(soltyp == rt_domain(did)%isoilwater) RT_DOMAIN(did)%VEGTYP = rt_domain(did)%iswater
         where(RT_DOMAIN(did)%VEGTYP == rt_domain(did)%iswater ) soltyp = rt_domain(did)%isoilwater

! LKSAT, 
! temporary set
       RT_DOMAIN(did)%SMCRTCHK = 0
       RT_DOMAIN(did)%SMCAGGRT = 0
       RT_DOMAIN(did)%STCAGGRT = 0
       RT_DOMAIN(did)%SH2OAGGRT = 0
     

       RT_DOMAIN(did)%zsoil(1:nlst_rt(did)%nsoil) = nlst_rt(did)%zsoil8(1:nlst_rt(did)%nsoil)

       RT_DOMAIN(did)%sldpth(1) = abs( RT_DOMAIN(did)%zsoil(1) )
       do i = 2, nlst_rt(did)%nsoil
          RT_DOMAIN(did)%sldpth(i) = RT_DOMAIN(did)%zsoil(i-1)-RT_DOMAIN(did)%zsoil(i)
       enddo
       RT_DOMAIN(did)%SOLDEPRT = -1.0*RT_DOMAIN(did)%ZSOIL(nlst_rt(did)%NSOIL)

       ierr_flg = 99
       if(trim(nlst_rt(did)%hydrotbl_f) == "") then
           call hydro_stop("FATAL ERROR: hydrotbl_f is empty. Please input a netcdf file. ")
       endif

       if(my_id .eq. IO_id) then
          ierr_flg = nf_open(trim(nlst_rt(did)%hydrotbl_f), NF_NOWRITE, ncid)
       endif
       call mpp_land_bcast_int1(ierr_flg)
       if( ierr_flg .ne. 0) then   
          ! input from HYDRO.tbl FILE
!      input OV_ROUGH from OVROUGH.TBL
       if(my_id .eq. IO_id) then

       open(71,file="HYDRO.TBL", form="formatted") 
!read OV_ROUGH first
          read(71,*) nn
          read(71,*)    
          do i = 1, nn
             read(71,*) RT_DOMAIN(did)%OV_ROUGH(i)
          end do 
!read parameter for LKSAT
          read(71,*) nn
          read(71,*)    
          do i = 1, nn
             read(71,*) xdum1(i), MAXSMC(i),refsmc(i),wltsmc(i)
          end do 
       close(71)

       endif
       call mpp_land_bcast_real(leng,RT_DOMAIN(did)%OV_ROUGH)
       call mpp_land_bcast_real(leng,xdum1)
       call mpp_land_bcast_real(leng,MAXSMC)
       call mpp_land_bcast_real(leng,refsmc)
       call mpp_land_bcast_real(leng,wltsmc)

       rt_domain(did)%lksat = 0.0
       do j = 1, RT_DOMAIN(did)%jx
             do i = 1, RT_DOMAIN(did)%ix
                !yw rt_domain(did)%lksat(i,j) = xdum1(soltyp(i,j) ) * 1000.0
                rt_domain(did)%lksat(i,j) = xdum1(soltyp(i,j) ) 
                IF(rt_domain(did)%VEGTYP(i,j) == rt_domain(did)%isurban ) THEN   ! urban
                    rt_domain(did)%SMCMAX1(i,j) = 0.45
                    rt_domain(did)%SMCREF1(i,j) = 0.42
                    rt_domain(did)%SMCWLT1(i,j) = 0.40
                else
                    rt_domain(did)%SMCMAX1(i,j) = MAXSMC(soltyp(I,J))
                    rt_domain(did)%SMCREF1(i,j) = refsmc(soltyp(I,J))
                    rt_domain(did)%SMCWLT1(i,j) = wltsmc(soltyp(I,J))
                    !ADCHANGE: Add some sanity checks in case calibration knocks the order of these out of sequence.
                    !The min diffs were pulled from the existing HYDRO.TBL defaults.
                    !Currently water is 0, so enforcing 0 as the absolute min.
                    rt_domain(did)%SMCMAX1(i,j) = min(rt_domain(did)%SMCMAX1(i,j), 1.0)
                    rt_domain(did)%SMCREF1(i,j) = max(min(rt_domain(did)%SMCREF1(i,j), rt_domain(did)%SMCMAX1(i,j) - 0.01), 0.0)
                    rt_domain(did)%SMCWLT1(i,j) = max(min(rt_domain(did)%SMCWLT1(i,j), rt_domain(did)%SMCREF1(i,j) - 0.01), 0.0)
                ENDIF
                IF(rt_domain(did)%VEGTYP(i,j) > 0 ) THEN   ! created 2d ov_rough
                    rt_domain(did)%OV_ROUGH2d(i,j) = RT_DOMAIN(did)%OV_ROUGH(rt_domain(did)%VEGTYP(I,J))
                endif
             end do
       end do

       call hdtbl_out(did)
    else
       ! input from HYDRO.TBL.nc file
       print*, "reading from hydrotbl_f(HYDRO.TBL.nc)  file ...."
       call hdtbl_in_nc(did)
    endif

       rt_domain(did)%soiltyp = soltyp
       
       if(allocated(soltyp)) deallocate(soltyp)


      end subroutine lsm_input


end module module_HYDRO_drv

! stop the job due to the fatal error.
      subroutine HYDRO_stop(msg)
        use module_mpp_land
        character(len=*) :: msg
        integer :: ierr
        ierr = 1
!#ifdef 1  !! PLEASE NEVER UNCOMMENT THIS IFDEF, it's just one incredibly useful string.
      write(6,*) "The job is stopped due to the fatal error. ", trim(msg)
      call flush(6)
!#endif

!        call mpp_land_sync()
!        write(my_id+90,*) msg
!        call flush(my_id+90)

         call mpp_land_abort()
         call MPI_finalize(ierr)

     return
     end  subroutine HYDRO_stop  


! stop the job due to the fatal error.
      subroutine HYDRO_finish()
        USE module_mpp_land
        use module_stream_nudging,  only: finish_stream_nudging
        
        integer :: ierr

        call finish_stream_nudging()
        print*, "The model finished successfully......."
!         call mpp_land_abort()
         call flush(6)
         call mpp_land_sync()
         call MPI_finalize(ierr)
         stop

        return
      end  subroutine HYDRO_finish
